package com.atguigu.adverStat


import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Hour, Minute}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object AdverStatStreamApp {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("AdverStatStreamApp")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val topics: String = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)
    val brokers: String = ConfigurationManager.config.getString(Constants.KAFKA_BROKER_LIST)

    val kafkaParams = Map(
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commint" -> (false: java.lang.Boolean)
    )

    val msgDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topics), kafkaParams))

    val adverDStream: DStream[String] = msgDStream.map(_.value())

    val filterAdverDStream: DStream[String] = adverDStream.transform(
      adverRDD => {

        val adBlackLists: Array[AdBlacklist] = AdBlacklistDAO.findAll()

        val blackList: Array[Long] = adBlackLists.map(_.userid)


        adverRDD.filter(adverStr => {

          !blackList.contains(adverStr.split(" ")(3).toLong)

        })
      }
    )

    //filterAdverDStream.foreachRDD(_.collect().foreach(println))


    //filterAdverDStream.foreachRDD(rdd=>rdd.collect().foreach(println))

    streamingContext.checkpoint("./stream-context")

    filterAdverDStream.checkpoint(Seconds(10))

    generateBlackList(filterAdverDStream)

    //各省统计值
    val adStat2CountDS: DStream[(AdStat, Long)] = provinceCityClickStat(filterAdverDStream)

    //统计各省的top3
    getProvinceTop3ClickCount(adStat2CountDS, sparkSession)


    //获取最近一小时数据量的统计
    getRecentHourClickCount(filterAdverDStream)


    streamingContext.start()
    streamingContext.awaitTermination()

  }

  def getRecentHourClickCount(adverDStream: DStream[String]) = {

    val adClickTrend2OneDS: DStream[(AdClickTrend, Long)] = adverDStream.map {
      log => {
        val logSplit: Array[String] = log.split(" ")
        val timestamp: Long = logSplit(0).toLong
        //yyyyMMddHHmm
        val time: String = DateUtils.formatTimeMinute(new Date(timestamp))
        val date: String = time.substring(0, 8)
        val hour: String = time.substring(8, 10)
        val minute: String = time.substring(10)
        val adid: Long = logSplit(4).toLong

        val adClickTrend: AdClickTrend = AdClickTrend(date, hour, minute, adid, 1L)

        (adClickTrend, 1L)
      }
    }

    val adClickTrend2CountDS: DStream[(AdClickTrend, Long)] = adClickTrend2OneDS.reduceByKeyAndWindow((a: Long, b: Long) => a + b, Minutes(60), Minutes(1))

    adClickTrend2CountDS.foreachRDD {
      adClickTrend2CountRDD => {
        adClickTrend2CountRDD.foreachPartition {
          iteratorAdClickTrend2Count => {

            val trends: Iterator[AdClickTrend] = iteratorAdClickTrend2Count.map {
              case (adClickTrend, count) => {
                adClickTrend.clickCount = count
                adClickTrend
              }
            }

            AdClickTrendDAO.updateBatch(trends.toArray)

          }


        }
      }
    }
  }


  def getProvinceTop3ClickCount(adStat2CountDS: DStream[(AdStat, Long)], sparkSession: SparkSession) = {

    val adProvinceClickCountDS: DStream[AdProvinceTop3] = adStat2CountDS.map {
      case (adStat, count) => {
        val adProvince: AdProvinceTop3 = AdProvinceTop3(adStat.date, adStat.province, adStat.adid, count)
        adProvince
      }
    }

    val adProvinceTop3DS: DStream[AdProvinceTop3] = adProvinceClickCountDS.transform {
      adProvinceClickCountRDD => {
        import sparkSession.implicits._
        adProvinceClickCountRDD.toDF().createOrReplaceTempView("tmp_ad_province_click_count")

        sparkSession
          .sql("select date,province,adid,sum(clickCount) as clickCount from tmp_ad_province_click_count group by date,province,adid")
          .createOrReplaceTempView("tmp_ad_province_click_count_1")

        val sql: String = "select " +
          "date," +
          "province," +
          "adid," +
          "clickCount " +
          "from " +
          "(select " +
          " * ," +
          "row_number() over (partition by  date,province order by clickCount desc) rn " +
          "from tmp_ad_province_click_count_1) a " +
          "where rn <=3"

        val adProvinceTop3RDD: RDD[AdProvinceTop3] = sparkSession.sql(sql).as[AdProvinceTop3].rdd
        adProvinceTop3RDD
      }
    }


    adProvinceTop3DS.foreachRDD {
      adProvinceTop3RDD =>
        adProvinceTop3RDD.foreachPartition {
          iteratorAdProvince =>
            AdProvinceTop3DAO.updateBatch(iteratorAdProvince.toArray)
        }
    }


  }

  def provinceCityClickStat(AdverDStream: DStream[String]): DStream[(AdStat, Long)] = {

    val adStat2OneDS: DStream[(AdStat, Long)] = AdverDStream.map(
      log => {
        val logSplit: Array[String] = log.split(" ")

        val timestamp: Long = logSplit(0).toLong
        val date: String = DateUtils.formatDateKey(new Date(timestamp))

        val province: String = logSplit(1)
        val city: String = logSplit(2)
        val adid: Long = logSplit(4).toLong

        val adStat = AdStat(date, province, city, adid, 1L)

        (adStat, 1L)

      }
    )


    val adStat2CountDS: DStream[(AdStat, Long)] = adStat2OneDS.updateStateByKey {
      case (seq: Seq[Long], option: Option[Long]) => {
        var count: Long = 0L
        if (option.isDefined) {
          count = option.get
        }

        val sum: Long = seq.sum
        count += sum

        Some(count)
      }
    }

    adStat2CountDS.foreachRDD {
      adStat2CountRDD => {
        adStat2CountRDD.foreachPartition {
          iterator: Iterator[(AdStat, Long)] => {

            val stats: Iterator[AdStat] = iterator.map(item => {
              item._1.clickCount = item._2
              item._1
            })

            AdStatDAO.updateBatch(stats.toArray)

          }
        }
      }
    }

    adStat2CountDS
  }

  def generateBlackList(adverDStream: DStream[String]) = {

    val userClick2OneDStream: DStream[(AdUserClickCount, Long)] = adverDStream.map(
      log => {
        val logSpilt: Array[String] = log.split(" ")

        val timestamp: Long = logSpilt(0).toLong

        val date: String = DateUtils.formatDateKey(new Date(timestamp))

        val userClick = AdUserClickCount(date, logSpilt(3).toLong, logSpilt(4).toLong, 0L)

        (userClick, 1L)
      }
    )

    val userClick2CountDStream: DStream[(AdUserClickCount, Long)] = userClick2OneDStream.reduceByKey(_ + _)

    userClick2CountDStream.foreachRDD(
      userClickCountRDD => {
        userClickCountRDD.foreachPartition(
          iteratorUserClickCount => {

            val userClickIterator: Iterator[AdUserClickCount] = iteratorUserClickCount.map {
              case (userClick, count) => {
                val clickCount: AdUserClickCount = userClick.copy()
                clickCount.clickCount = count
                clickCount
              }
            }

            AdUserClickCountDAO.updateBatch(userClickIterator.toArray)
          }
        )
      }
    )


    val blcakListDStream: DStream[(AdUserClickCount, Long)] = userClick2CountDStream.filter {
      case (userClickCount, count) => {
        val cnt: Int = AdUserClickCountDAO.findClickCountByMultiKey(userClickCount.date, userClickCount.userid, userClickCount.adid)

        val bool: Boolean = cnt > 100
        bool
      }
    }


    val userIdDStream: DStream[Long] = blcakListDStream.map(_._1.userid).transform(rdd => rdd.distinct())

    userIdDStream.map(AdBlacklist(_)).foreachRDD(blackListRDD => blackListRDD.foreachPartition {
      iteratorBlackList => {
        AdBlacklistDAO.insertBatch(iteratorBlackList.toArray)
      }
    })


  }
}

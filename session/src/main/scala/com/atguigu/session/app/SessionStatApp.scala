package com.atguigu.session.app

import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, NumberUtils, ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random


object SessionStatApp {

  def main(args: Array[String]): Unit = {
    val taskParams: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    val taskParamsJsonObject: JSONObject = JSONObject.fromObject(taskParams)

    val taskId: String = UUID.randomUUID().toString


    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("session")

    val ss: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()


    val actionRDD: RDD[UserVisitAction] = getActionRDD(ss, taskParamsJsonObject)

    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(action => (action.session_id, action))

    val session2GroupbyAction: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()


    session2GroupbyAction.cache()

    val userId2AggrInfoRDD: RDD[(Long, String)] = getSessonFullInfo(session2GroupbyAction)


    //获取user表
    val userId2userInfoRDD: RDD[(Long, UserInfo)] = getUserRDD(ss)

    //关联user表
    val sessionId2FullInfoRDD: RDD[(String, String)] = getSessonJoinUser(userId2AggrInfoRDD, userId2userInfoRDD)

    val accumulator: SessionAccumulator = new SessionAccumulator()

    ss.sparkContext.register(accumulator)
    val filerRDD: RDD[(String, String)] = getFilterFullInfoRDD(ss, sessionId2FullInfoRDD, taskParamsJsonObject, accumulator)

    filerRDD.cache()
    filerRDD.foreach(println)

    val sessionMap: mutable.HashMap[String, Int] = accumulator.value

    getSessionRatio(ss, taskId, sessionMap)

    //计算比率


    //val value: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
    //value.collect().foreach(println)

    //需求二抽样
    sessionRandomExtract(ss, taskId, filerRDD)


    //需求三 top 10
    val sessionId2UserVisitAction: RDD[(String, UserVisitAction)] = sessionId2ActionRDD.join(filerRDD).map {
      case (sessionId, (userVisitAction: UserVisitAction, fullInfo)) => {
        (sessionId, userVisitAction)
      }
    }

    sessionId2UserVisitAction.cache()

    val sorKey2CategoryId: Array[(SortKey, Long)] = getCategoryTop10(ss, taskId, sessionId2UserVisitAction)


    //需求4 点击了top10 category 的top 10 Session
    getClieckTop10CategorySessionTop(ss, taskId, sorKey2CategoryId, sessionId2UserVisitAction)

    ss.stop()


  }

  def getClieckTop10CategorySessionTop(ss: SparkSession, taskId: String,
                                       sorKey2CategoryId: Array[(SortKey, Long)],
                                       sessionId2UserVisitAction: RDD[(String, UserVisitAction)]): Unit = {

    val categoryIds: List[Long] = sorKey2CategoryId.toMap.values.toList

    val categoryIdsBC: Broadcast[List[Long]] = ss.sparkContext.broadcast(categoryIds)

    val clickSessionId2UserVisitAction: RDD[(String, UserVisitAction)] = sessionId2UserVisitAction.filter(item => categoryIdsBC.value.contains(item._2.click_category_id))

    val session2IterableAction: RDD[(String, Iterable[UserVisitAction])] = clickSessionId2UserVisitAction.groupByKey()

    val catagoryId2SessionCount: RDD[(Long, String)] = session2IterableAction.flatMap {
      case (sessionId, iterableAction) => {
        val categoryId2Count = new mutable.HashMap[Long, Long]()
        for (action <- iterableAction) {
          categoryId2Count += (action.click_category_id -> (categoryId2Count.getOrElse(action.click_category_id, 0L).toLong + 1))
        }

        categoryId2Count.map(item => (item._1, sessionId + "=" + item._2))
      }
    }

    val categoryId2IterableSessionCount: RDD[(Long, Iterable[String])] = catagoryId2SessionCount.groupByKey()

    val top10SessionRDD: RDD[Top10Session] = categoryId2IterableSessionCount.flatMap {
      case (categoryId, iterableSessionCount) => {
        val top10Session: List[String] = iterableSessionCount.toList
          .sortWith((action1, action2) => action1.split("=")(1).toLong > action2.split("=")(1).toLong).take(10)

        var orderRn: Int = 1
        top10Session.map {
          sessionCount =>
            val sc: Array[String] = sessionCount.split("=")
            val sessionId: String = sc(0)
            val clickCount: Long = sc(1).toLong
            val session = Top10Session(taskId, categoryId, orderRn, sessionId, clickCount)
            orderRn += 1
            session
        }
      }
    }
    import ss.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","top10_session")
      .mode(SaveMode.Append)
      .save()

  }


  //top10
  def getCategoryTop10(ss: SparkSession, taskId: String, sessionId2UserVisitAction: RDD[(String, UserVisitAction)]): Array[(SortKey, Long)] = {


    //获取所有被下单 点击 支付过商品
    val categoryId2categoryId: RDD[(Long, Long)] = sessionId2UserVisitAction.flatMap {
      case (sessionId, userVisitAction: UserVisitAction) => {
        val categoryIdArr = new ArrayBuffer[(Long, Long)]()

        if (userVisitAction.click_category_id != -1) {
          categoryIdArr.append((userVisitAction.click_category_id, userVisitAction.click_category_id))
        } else if (userVisitAction.order_category_ids != null) {
          val categoryIds: Array[String] = userVisitAction.order_category_ids.split(",")
          for (categoryId <- categoryIds) {
            categoryIdArr.append((categoryId.toLong, categoryId.toLong))
          }

        } else if (userVisitAction.pay_category_ids != null) {
          val categoryIds: Array[String] = userVisitAction.pay_category_ids.split(",")
          for (categoryId <- categoryIds) {
            categoryIdArr.append((categoryId.toLong, categoryId.toLong))
          }
        }


        categoryIdArr
      }

    }
    val distianctCategory: RDD[(Long, Long)] = categoryId2categoryId.distinct()

    //获取点击次数
    val categoryClickCountRDD: RDD[(Long, Long)] = getClickCategoryCount(sessionId2UserVisitAction)

    //下单次数
    val categoryOrderCountRDD: RDD[(Long, Long)] = getOrderCategoryCount(sessionId2UserVisitAction)

    //获取支付次数
    val categoryPayCountRDD: RDD[(Long, Long)] = getPayCategoryCount(sessionId2UserVisitAction)

    //关联
    val fullCountRDD: RDD[(Long, String)] = getFullCountRDD(distianctCategory, categoryClickCountRDD, categoryOrderCountRDD, categoryPayCountRDD)

    val sorKey2CategoryIdRDD: RDD[(SortKey, Long)] = fullCountRDD.map {
      case (categoryId, aggrInfo) => {
        val clickCount: Long = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount: Long = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount: Long = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey: SortKey = SortKey(clickCount, orderCount, payCount)

        (sortKey, categoryId)
      }
    }

    val sorKey2CategoryId: Array[(SortKey, Long)] = sorKey2CategoryIdRDD.sortByKey(false).take(10)

    val resultRDD: RDD[(SortKey, Long)] = ss.sparkContext.makeRDD(sorKey2CategoryId)

    val Top10CategoryRDD: RDD[Top10Category] = resultRDD.map {
      case (sorKey, categoryId) => {
        Top10Category(taskId, categoryId, sorKey.clickCount, sorKey.orderCount, sorKey.payCount)
      }
    }

    import ss.implicits._
    Top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category")
      .mode(SaveMode.Append)
      .save()

    sorKey2CategoryId
  }

  def getFullCountRDD(distianctCategory: RDD[(Long, Long)],
                      categoryClickCountRDD: RDD[(Long, Long)],
                      categoryOrderCountRDD: RDD[(Long, Long)],
                      categoryPayCountRDD: RDD[(Long, Long)]): RDD[(Long, String)] = {

    val cirId2ClickCountRDD: RDD[(Long, String)] = distianctCategory.leftOuterJoin(categoryClickCountRDD).map {
      case (criId, (categryId, option)) => {
        val clickCount: Long = if (option.isDefined) option.get else 0

        val aggrInfo: String = Constants.FIELD_CATEGORY_ID + "=" + criId + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount + "|"

        (criId, aggrInfo)
      }
    }

    val criId2OrderCount: RDD[(Long, String)] = cirId2ClickCountRDD.leftOuterJoin(categoryOrderCountRDD).map {
      case (criId, (aggrInfo, option)) => {
        val orderCount: Long = if (option.isDefined) option.get else 0

        val orderAggrInfo: String = aggrInfo + Constants.FIELD_ORDER_COUNT + "=" + orderCount + "|"
        (criId, orderAggrInfo)
      }
    }

    val fullCountRDD: RDD[(Long, String)] = criId2OrderCount.leftOuterJoin(categoryPayCountRDD).map {
      case (criId, (aggrInfo, option)) => {
        val PayCount: Long = if (option.isDefined) option.get else 0

        val payAggrInfo: String = aggrInfo + Constants.FIELD_PAY_COUNT + "=" + PayCount + "|"
        (criId, payAggrInfo)
      }
    }
    fullCountRDD
  }

  def getClickCategoryCount(sessionId2UserVisitAction: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    val clickActionRDD: RDD[(String, UserVisitAction)] = sessionId2UserVisitAction.filter {
      case (sessionId, userVisitAction: UserVisitAction) => userVisitAction.click_category_id != -1
    }

    val categoryId2one: RDD[(Long, Long)] = clickActionRDD.map(item => (item._2.click_category_id, 1L))

    val categoryClickCountRDD: RDD[(Long, Long)] = categoryId2one.reduceByKey(_ + _)

    categoryClickCountRDD
  }

  def getOrderCategoryCount(sessionId2UserVisitAction: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    val orderActionRDD: RDD[(String, UserVisitAction)] = sessionId2UserVisitAction.filter(_._2.order_category_ids != null)

    val categoryId2one: RDD[(Long, Long)] = orderActionRDD.flatMap {
      case (sessionId, userVisitAction: UserVisitAction) => {
        val categoryIdArr = new ArrayBuffer[(Long, Long)]()
        val categoryIds: Array[String] = userVisitAction.order_category_ids.split(",")
        for (categoryId <- categoryIds) {
          categoryIdArr.append((categoryId.toLong, 1L))
        }
        categoryIdArr
      }
    }

    val categoryOrderCountRDD: RDD[(Long, Long)] = categoryId2one.reduceByKey(_ + _)
    categoryOrderCountRDD
  }

  def getPayCategoryCount(sessionId2UserVisitAction: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    val payActionRDD: RDD[(String, UserVisitAction)] = sessionId2UserVisitAction.filter(_._2.pay_category_ids != null)

    val categoryId2one: RDD[(Long, Long)] = payActionRDD.flatMap {
      case (sessionId, userVisitAction: UserVisitAction) => {
        val categoryIdArr = new ArrayBuffer[(Long, Long)]()
        val categoryIds: Array[String] = userVisitAction.pay_category_ids.split(",")
        for (categoryId <- categoryIds) {
          categoryIdArr.append((categoryId.toLong, 1L))
        }
        categoryIdArr
      }
    }

    val categoryPayCountRDD: RDD[(Long, Long)] = categoryId2one.reduceByKey(_ + _)
    categoryPayCountRDD
  }

  //随机抽取数量
  def sessionRandomExtract(ss: SparkSession, taskId: String, filerRDD: RDD[(String, String)]): Unit = {

    val startDateHour2FullInfoRDD: RDD[(String, String)] = filerRDD.map {
      case (sessionId, fullInfo) => {
        val startTime: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)

        val startDateHour: String = DateUtils.getDateHour(startTime)


        (startDateHour, fullInfo)
      }
    }

    val startDateHour2CountMap: collection.Map[String, Long] = startDateHour2FullInfoRDD.countByKey()

    val date2HourMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((dateHour, cnt) <- startDateHour2CountMap) {
      val dates: Array[String] = dateHour.split("_")

      val day: String = dates(0)
      val hour: String = dates(1)

      date2HourMap.get(day) match {
        case None => date2HourMap(day) = new mutable.HashMap[String, Long]()
          date2HourMap(day) += (hour -> cnt)
        case Some(map) => date2HourMap(day) += (hour -> cnt)
      }
    }

    //要抽100条数据 每天抽多少条
    val extractPreDay: Int = 100 / date2HourMap.size

    println("--------------")
    println(date2HourMap.size)
    println(extractPreDay)
    println(date2HourMap)

    // 每个小时抽多少条


    val dateHourExtractListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    for ((day, hourMap) <- date2HourMap) {

      val daySum: Long = hourMap.values.sum

      dateHourExtractListMap.get(day) match {
        case None => dateHourExtractListMap(day) = new mutable.HashMap[String, ListBuffer[Int]]()
          getRandomIndexListMap(dateHourExtractListMap(day), daySum, hourMap, extractPreDay)
        case Some(map) =>
          getRandomIndexListMap(dateHourExtractListMap(day), daySum, hourMap, extractPreDay)
      }

    }


    println(dateHourExtractListMap)
    val hourIndexListBC: Broadcast[mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]] = ss.sparkContext.broadcast(dateHourExtractListMap)

    val dateHour2ItetableFullInfo: RDD[(String, Iterable[String])] = startDateHour2FullInfoRDD.groupByKey()

    val sessionRandomExtractRDD: RDD[SessionRandomExtract] = dateHour2ItetableFullInfo.flatMap {
      case (dateHour, itetableFullInfo) => {
        val ds: mutable.ArrayOps[String] = dateHour.split("_")
        val day: String = ds(0)
        val hour: String = ds(1)
        val indexs: ListBuffer[Int] = hourIndexListBC.value.get(day).get(hour)

        val sessions = new ArrayBuffer[SessionRandomExtract]()

        var index = 0

        for (fullInfo <- itetableFullInfo) {
          if (indexs.contains(index)) {
            //sessionid:String,
            val sessionid: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
            //StartTime:String,
            val startTime: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
            //SearchKeywords:String,
            val searchKeywords: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            //clickCategoryIds:String
            val clickCategoryIds: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

            val extract = SessionRandomExtract(taskId, sessionid, startTime, searchKeywords, clickCategoryIds)

            sessions.append(extract)
          }
          index += 1
        }

        sessions
      }
    }
    import ss.implicits._
    sessionRandomExtractRDD.toDF().write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_random_extract")
      .mode(SaveMode.Append)
      .save()

  }

  //生成随机的index
  def getRandomIndexListMap(randomListMap: mutable.HashMap[String, ListBuffer[Int]],
                            daySum: Long,
                            hourMap: mutable.HashMap[String, Long],
                            extractPreDay: Int): Unit = {
    for ((hour, cnt) <- hourMap) {
      var extractHourCnt: Long = ((cnt / daySum.toDouble) * extractPreDay).toLong
      if (extractHourCnt > daySum) {
        extractHourCnt = daySum
      }

      val random = new Random()

      randomListMap.get(hour) match {
        case None => randomListMap(hour) = new ListBuffer[Int]()
          for (i <- 0 to extractHourCnt.toInt) {
            var index: Int = random.nextInt(cnt.toInt)
            println(cnt)
            while (randomListMap(hour).contains(index)) {
              index = random.nextInt(cnt.toInt)

            }
            println(index)
            randomListMap(hour).append(index)
          }
        case Some(list) =>
          for (i <- 0 to extractHourCnt.toInt) {
            var index: Int = random.nextInt(cnt.toInt)
            while (randomListMap(hour).contains(index)) {
              index = random.nextInt(cnt.toInt)
            }
            randomListMap(hour).append(index)
          }
      }

    }

  }


  def getSessionRatio(ss: SparkSession, taskId: String, sessionMap: mutable.HashMap[String, Int]): Unit = {
    val sessionCount: Double = sessionMap.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visitLength_1s_3s: Int = sessionMap.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visitLength_4s_6s: Int = sessionMap.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visitLength_7s_9s: Int = sessionMap.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visitLength_10s_30s: Int = sessionMap.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visitLength_30s_60s: Int = sessionMap.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visitLength_1m_3m: Int = sessionMap.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visitLength_3m_10m: Int = sessionMap.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visitLength_10m_30m: Int = sessionMap.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visitLength_30m: Int = sessionMap.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val stepLength_1_3: Int = sessionMap.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val stepLength_4_6: Int = sessionMap.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val stepLength_7_9: Int = sessionMap.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val stepLength_10_30: Int = sessionMap.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val stepLength_30_60: Int = sessionMap.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val stepLength_60: Int = sessionMap.getOrElse(Constants.STEP_PERIOD_60, 0)


    val visitLength_1s_3sRatio: Double = NumberUtils.formatDouble(visitLength_1s_3s / sessionCount, 2)
    val visitLength_4s_6sRatio: Double = NumberUtils.formatDouble(visitLength_4s_6s / sessionCount, 2)
    val visitLength_7s_9sRatio: Double = NumberUtils.formatDouble(visitLength_7s_9s / sessionCount, 2)
    val visitLength_10s_30sRatio: Double = NumberUtils.formatDouble(visitLength_10s_30s / sessionCount, 2)
    val visitLength_30s_60sRatio: Double = NumberUtils.formatDouble(visitLength_30s_60s / sessionCount, 2)
    val visitLength_1m_3mRatio: Double = NumberUtils.formatDouble(visitLength_1m_3m / sessionCount, 2)
    val visitLength_3m_10mRatio: Double = NumberUtils.formatDouble(visitLength_3m_10m / sessionCount, 2)
    val visitLength_10m_30mRatio: Double = NumberUtils.formatDouble(visitLength_10m_30m / sessionCount, 2)
    val visitLength_30mRatio: Double = NumberUtils.formatDouble(visitLength_30m / sessionCount, 2)


    val stepLength_1_3Ratio: Double = NumberUtils.formatDouble(stepLength_1_3 / sessionCount, 2)
    val stepLength_4_6Ratio: Double = NumberUtils.formatDouble(stepLength_4_6 / sessionCount, 2)
    val stepLength_7_9Ratio: Double = NumberUtils.formatDouble(stepLength_7_9 / sessionCount, 2)
    val stepLength_10_30Ratio: Double = NumberUtils.formatDouble(stepLength_10_30 / sessionCount, 2)
    val stepLength_30_60Ratio: Double = NumberUtils.formatDouble(stepLength_30_60 / sessionCount, 2)
    val stepLength_60Ratio: Double = NumberUtils.formatDouble(stepLength_60 / sessionCount, 2)

    val stat: SessionAggrStat = new SessionAggrStat(taskId,
      sessionCount.toLong,
      visitLength_1s_3sRatio,
      visitLength_4s_6sRatio,
      visitLength_7s_9sRatio,
      visitLength_10s_30sRatio,
      visitLength_30s_60sRatio,
      visitLength_1m_3mRatio,
      visitLength_3m_10mRatio,
      visitLength_10m_30mRatio,
      visitLength_30mRatio,
      stepLength_1_3Ratio,
      stepLength_4_6Ratio,
      stepLength_7_9Ratio,
      stepLength_10_30Ratio,
      stepLength_30_60Ratio,
      stepLength_60Ratio
    )

    import ss.implicits._
    val statDF: DataFrame = ss.sparkContext.makeRDD(Array(stat), 1).toDF()

    statDF.write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio")
      .mode(SaveMode.Append)
      .save()
  }

  def getFilterFullInfoRDD(ss: SparkSession, sessionId2FullInfoRDD: RDD[(String, String)], taskParamsJsonObject: JSONObject, accumulator: SessionAccumulator): RDD[(String, String)] = {

    val startAge: String = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_AGE)
    val endAge: String = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_AGE)
    val professionals: String = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_PROFESSIONALS)
    val cities: String = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_CITIES)
    val sex: String = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_SEX)
    val keyWrods: String = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_KEYWORDS)
    val categoryIds: String = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_CATEGORY_IDS)

    var filerInfo: String = (if (StringUtils.isNotEmpty(startAge)) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (StringUtils.isNotEmpty(endAge)) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (StringUtils.isNotEmpty(professionals)) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (StringUtils.isNotEmpty(cities)) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (StringUtils.isNotEmpty(sex)) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (StringUtils.isNotEmpty(keyWrods)) Constants.PARAM_KEYWORDS + "=" + keyWrods + "|" else "") +
      (if (StringUtils.isNotEmpty(categoryIds)) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" else "")

    if (filerInfo.endsWith("|")) {
      filerInfo = filerInfo.substring(0, filerInfo.length - 1)
    }

    println(filerInfo)

    val filterInfoBC: Broadcast[String] = ss.sparkContext.broadcast(filerInfo)


    val filerRDD: RDD[(String, String)] = sessionId2FullInfoRDD.filter {
      case (session_id, fullInfo) => {
        var success = true

        val filterInfo: String = filterInfoBC.value

        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {

          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {

          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {

          success = false
        } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {

          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {

          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {

          success = false
        }


        //累加器计数
        if (success) {
          accumulator.add(Constants.SESSION_COUNT)

          //获取时长
          val visitLength: Long = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          //获取步长
          val stepLength: Long = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          addVisitLength(visitLength, accumulator)
          addStepLength(stepLength, accumulator)

        }

        success
      }
    }
    filerRDD

  }

  //累加时长
  def addVisitLength(visitLength: Long, accumulator: SessionAccumulator): Unit = {
    if (visitLength >= 1 && visitLength <= 3) {
      accumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      accumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      accumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength < 30) {
      accumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength >= 30 && visitLength < 60) {
      accumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength >= 60 && visitLength < 180) {
      accumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength >= 180 && visitLength < 600) {
      accumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength >= 600 && visitLength < 1800) {
      accumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength >= 1800) {
      accumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  //累加步长
  def addStepLength(stepLength: Long, accumulator: SessionAccumulator): Unit = {
    if (stepLength >= 1 && stepLength <= 3) {
      accumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      accumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      accumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength < 30) {
      accumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength >= 30 && stepLength <= 60) {
      accumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength >= 60) {
      accumulator.add(Constants.STEP_PERIOD_4_6)
    }
  }

  def getSessonJoinUser(userId2AggrInfoRDD: RDD[(Long, String)], userId2userInfoRDD: RDD[(Long, UserInfo)]): RDD[(String, String)] = {

    userId2AggrInfoRDD.join(userId2userInfoRDD).map {
      case (userId, (aggrInfo, userInfo)) => {
        val fullInfo: String = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + userInfo.age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
          Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
          Constants.FIELD_CITY + "=" + userInfo.city
        val sessionId: String = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
      }
    }

  }


  def getUserRDD(ss: SparkSession): RDD[(Long, UserInfo)] = {

    val sql = "select * from user_info"
    import ss.implicits._
    val userRDD: RDD[(Long, UserInfo)] = ss.sql(sql).as[UserInfo].rdd.map(user => (user.user_id, user))

    userRDD

  }

  def getSessonFullInfo(session2GroupbyAction: RDD[(String, Iterable[UserVisitAction])]): RDD[(Long, String)] = {
    val userId2AggrInfoRDD: RDD[(Long, String)] = session2GroupbyAction.map {
      case (sessionId, iterableAction) => {

        val searchKeywords = new StringBuffer("")
        val clickCategoryId = new StringBuffer("")

        var visitLength: Long = 0L
        var stepLength: Int = 0

        var startTime: Date = DateUtils.parseTime("9999-01-01 00:00:00");
        var endTime: Date = DateUtils.parseTime("2000-01-01 00:00:00");

        var userId: Long = -1L

        for (action: UserVisitAction <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id;
          }

          if (StringUtils.isNotEmpty(action.search_keyword) && !searchKeywords.toString.contains(action.search_keyword)) {
            searchKeywords.append(action.search_keyword + ",")
          }

          if (StringUtils.isNotEmpty(action.click_category_id.toString) && !clickCategoryId.toString.contains(action.click_category_id.toString)) {
            clickCategoryId.append(action.click_category_id + ",")
          }

          val actionTime: Date = DateUtils.parseTime(action.action_time)
          if (startTime.after(actionTime)) {
            startTime = actionTime
          }

          if (actionTime.after(endTime)) {
            endTime = actionTime
          }

          stepLength += 1

        }

        val searchKw: String = StringUtils.trimComma(searchKeywords.toString)
        val clickCi: String = StringUtils.trimComma(clickCategoryId.toString)
        visitLength = (endTime.getTime - startTime.getTime) / 1000


        val aggrInfo: String = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryId + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|"


        (userId, aggrInfo)

      }
    }
    userId2AggrInfoRDD


  }


  def getActionRDD(ss: SparkSession, taskParamsJsonObject: JSONObject): RDD[UserVisitAction] = {
    val startDate: String = taskParamsJsonObject.getString(Constants.PARAM_START_DATE)
    val endDate: String = taskParamsJsonObject.getString(Constants.PARAM_END_DATE)

    val sql = s"select * from user_visit_action where date>'${startDate}' and date<'${endDate}'"

    import ss.implicits._
    val actionRDD: RDD[UserVisitAction] = ss.sql(sql).as[UserVisitAction].rdd


    actionRDD
  }

}

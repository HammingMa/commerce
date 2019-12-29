package com.atguigu.areaStat

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}


object AreaTop3StatApp {




  def main(args: Array[String]): Unit = {

    val params: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParams: JSONObject = JSONObject.fromObject(params)

    val taskId: String = UUID.randomUUID().toString

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AreaTop3App")

    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val city2ProductIdRDD: RDD[(Long, String)] = getCity2ProductIdRDD(spark, taskParams)

    val cityId2CityInfo: RDD[(Long, CityInfo)] = getCityAreaInfo(spark)

    getAreaPidBaseInfo(spark, city2ProductIdRDD, cityId2CityInfo)


    spark.udf.register("concat_city_info", (cityId: Long, cityName: String, spliter: String) => {
      cityId + spliter + cityName
    })

    val groupConcatDistinct = new GroupConcatDistinct()
    spark.udf.register("group_concat_distinct", groupConcatDistinct)

    getAreaProudctClickCountTable(spark)

    spark.udf.register("get_json_field", (json: String, field: String) => {
      JSONObject.fromObject(json).getString(field)
    })

    getAreaProductInfoClickCountTable(spark)


    getAreaTop3Product(spark)

    spark.sql("select * from tmp_area_proudct_info_click_count").show()


  }

  def getAreaTop3Product(spark: SparkSession) = {
    val sql: String = "select " +
      "area," +
      "area_level," +
      "city_info," +
      "pid," +
      "product_name," +
      "product_status," +
      "click_count," +
      "rn " +
      "from " +
      "(select " +
      "*," +
      "row_number() over (partition by area order by click_count desc ) as rn " +
      "from tmp_area_proudct_info_click_count) a " +
      "where a.rn <=3"

    spark.sql(sql).write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","area_top3_product")
      .mode(SaveMode.Append)
      .save()


  }

  def getAreaProductInfoClickCountTable(spark: SparkSession) = {
    val sql: String = "select " +
      "a.area," +
      "case a.area" +
      " when '华北' then 'A_level' " +
      " when '华东' then 'A_level' " +
      " when '华中' then 'B_level' " +
      " when '华南' then 'B_level' " +
      " when '西南' then 'C_level' " +
      " when '西北' then 'C_level' " +
      " else 'D_level' end " +
      "as area_level,"+
      "a.pid," +
      "a.city_info," +
      "a.click_count," +
      "b.product_name," +
      "if(get_json_field(b.extend_info,'product_status')= '0','Self','Thrid Party') as product_status" +
      " from area_product_click_count a inner join product_info b on a.pid = b.product_id "


    spark.sql(sql).createOrReplaceTempView("tmp_area_proudct_info_click_count")
  }

  def getAreaProudctClickCountTable(spark: SparkSession): Unit = {
    val sql = "select area,pid,group_concat_distinct(concat_city_info(city_id,city_name,':')) as city_info,  count(*) as click_count from area_proudct_base_info group by area,pid"

    spark.sql(sql).createOrReplaceTempView("area_product_click_count")

  }

  def getAreaPidBaseInfo(spark: SparkSession,
                         city2ProductIdRDD: RDD[(Long, String)],
                         cityId2CityInfo: RDD[(Long, CityInfo)]): Unit = {

    val area2ProductRDD: RDD[(Long, String, String, String)] = city2ProductIdRDD.join(cityId2CityInfo).map {
      case (cityId, (pid, cityInfo)) => {
        (cityId, cityInfo.city_name, cityInfo.area, pid)
      }
    }

    import spark.implicits._

    area2ProductRDD.toDF("city_id", "city_name", "area", "pid").createOrReplaceTempView("area_proudct_base_info")

  }

  def getCityAreaInfo(spark: SparkSession): RDD[(Long, CityInfo)] = {
    val cityInfo = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"), (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"), (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"), (9L, "哈尔滨", "东北"))


    val cityId2CityInfo: RDD[(Long, CityInfo)] = spark.sparkContext.makeRDD(cityInfo).map(item => (item._1, CityInfo(item._1, item._2, item._3)))
    cityId2CityInfo
  }

  def getCity2ProductIdRDD(spark: SparkSession, taskParams: JSONObject): RDD[(Long, String)] = {
    val startDate: String = taskParams.getString(Constants.PARAM_START_DATE)
    val endDate: String = taskParams.getString(Constants.PARAM_END_DATE)

    val sql = s"select city_id,click_product_id from user_visit_action where click_product_id <> -1 and date >= '$startDate' and date <='${endDate}' "

    import spark.implicits._
    val city2ProductIdRDD: RDD[(Long, String)] = spark.sql(sql).as[CityClickProduct].rdd.map(cityClickProduct => (cityClickProduct.city_id.toLong, cityClickProduct.click_product_id))

    city2ProductIdRDD
  }

}

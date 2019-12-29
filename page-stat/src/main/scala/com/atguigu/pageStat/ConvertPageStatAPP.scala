package com.atguigu.pageStat


import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.DateUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object ConvertPageStatAPP {



  def main(args: Array[String]): Unit = {
    val taskParams: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    val taskParamsObject: JSONObject = JSONObject.fromObject(taskParams)

    val taskId: String = UUID.randomUUID().toString

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ConvertPageStatAPP")

    val ss: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val sessionId2UserVisitAction: RDD[(String, UserVisitAction)] = getUserVisitActionRDD(ss, taskParamsObject)

    //sessionId2UserVisitAction.collect().foreach(println)

    val pageFlow: Array[String] = taskParamsObject.getString(Constants.PARAM_TARGET_PAGE_FLOW).split(",")

    val pageSplit: Array[String] = pageFlow.slice(0, pageFlow.length - 1).zip(pageFlow.tail).map(item => item._1 + "_" + item._2)

    val pageSplitBC: Broadcast[Array[String]] = ss.sparkContext.broadcast(pageSplit)

    val sessionId2IterableAction: RDD[(String, Iterable[UserVisitAction])] = sessionId2UserVisitAction.groupByKey()

    val pageFlow2OneRDD: RDD[(String, Int)] = sessionId2IterableAction.flatMap {
      case (sessionId, iterableAction) => {

        val sortAction: List[UserVisitAction] = iterableAction.toList.sortWith {
          case (action1, action2) => DateUtils.parseTime(action1.action_time).getTime < DateUtils.parseTime(action2.action_time).getTime
        }

        val sortPageIds: List[Long] = sortAction.map(_.page_id)

        val pageFlows: List[String] = sortPageIds.slice(0, sortPageIds.length - 1).zip(sortPageIds.tail).map(item => item._1 + "_" + item._2)

        val filterPageFlow: List[String] = pageFlows.filter(pageSplitBC.value.contains(_))

        val pageFlow2One: List[(String, Int)] = filterPageFlow.map((_, 1))

        pageFlow2One

      }
    }

    val pageFlow2Count: collection.Map[String, Long] = pageFlow2OneRDD.countByKey()

    val firstPageCount: Long = sessionId2UserVisitAction.filter(pageFlow(0).toLong==_._2.page_id).count()

    println(pageFlow2Count)

    //getPageFlowRatio(ss,taskId,pageSplit,pageFlow2Count,firstPageCount)

    ss.stop()

  }

  def getPageFlowRatio(ss: SparkSession, taskId: String,pageSplit: Array[String], pageFlow2Count: collection.Map[String, Long], firstPageCount: Long) = {

    var lastPageCount: Long = firstPageCount

    val pageSplit2Ratio = new mutable.HashMap[String,Double]()

    for (ps <- pageSplit) {
      val currentPageCount: Long = pageFlow2Count.get(ps).get
      val ratio: Double = currentPageCount/lastPageCount.toDouble
      pageSplit2Ratio.put(ps,ratio)
      lastPageCount=currentPageCount
    }

    val pageSplitRatios: mutable.Iterable[PageSplitRatio] = pageSplit2Ratio.map{ case (pageSplit,ratio) => PageSplitRatio(taskId,pageSplit,ratio)}

    import ss.implicits._
    ss.sparkContext.makeRDD(pageSplitRatios.toList).toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","page_split_ratio")
      .mode(SaveMode.Append)
      .save()




  }


  def getUserVisitActionRDD(ss: SparkSession, taskParamsObject: JSONObject): RDD[(String, UserVisitAction)] = {
    val startDate: String = taskParamsObject.getString(Constants.PARAM_START_DATE)
    val endDate: String = taskParamsObject.getString(Constants.PARAM_END_DATE)

    val sql: String = s"select * from user_visit_action where date >= '${startDate}' and date <= '${endDate}'"

    println(sql)

    import ss.implicits._
    val sessionId2UserVisitAction: RDD[(String, UserVisitAction)] = ss.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))
    sessionId2UserVisitAction
  }

}

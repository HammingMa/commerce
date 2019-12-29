package com.atguigu.session.app

import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable


object SessionStatAggregateByKeyApp {

  def main(args: Array[String]): Unit = {
    val taskParams: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    val taskParamsJsonObject: JSONObject = JSONObject.fromObject(taskParams)

    val taskId: String = UUID.randomUUID().toString


    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("session")

    val ss: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()


    val actionRDD: RDD[UserVisitAction] = getActionRDD(ss, taskParamsJsonObject)

    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(action => (action.session_id, action))

    //val session2GroupbyAction: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()
    //
    //
    //session2GroupbyAction.cache()
    //
    //val userId2AggrInfoRDD: RDD[(Long, String)] = getSessonFullInfo(session2GroupbyAction)

    val aggrInfo: String = Constants.FIELD_SESSION_ID + "=" + -1L + "|" +
      Constants.FIELD_USER_ID + "=" + -1L + "|" +
      Constants.FIELD_SEARCH_KEYWORDS + "=" + "|" +
      Constants.FIELD_CLICK_CATEGORY_IDS + "=" + "|" +
      Constants.FIELD_VISIT_LENGTH + "=" + 0 + "|" +
      Constants.FIELD_STEP_LENGTH + "=" + 0 + "|" +
      Constants.FIELD_START_TIME + "=" + "9999-01-01 00:00:00" + "|" +
      Constants.FIELD_END_TIME + "=" + "2000-01-01 00:00:00"

    val session2AggrStr: RDD[(String, String)] = sessionId2ActionRDD.aggregateByKey(aggrInfo)(
      (aggrInfo: String, userVisitAction: UserVisitAction) => {

        val user_id: Long = userVisitAction.user_id
        val session_id: String = userVisitAction.session_id

        var searchKeyword: String = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)

        var clickCategoryId: String = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

        //var visitLength: Long = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
        var stepLength: Int = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toInt

        var startTime: Date = DateUtils.parseTime(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME));
        var endTime: Date = DateUtils.parseTime(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_END_TIME));


        if (StringUtils.isEmpty(searchKeyword) || searchKeyword == null) {
          searchKeyword = ""
        }
        val searchKeywords = new StringBuffer("searchKeyword")
        if (StringUtils.isNotEmpty(userVisitAction.search_keyword) && !searchKeywords.toString.contains(userVisitAction.search_keyword)) {
          searchKeywords.append(userVisitAction.search_keyword + ",")
        }


        if (StringUtils.isEmpty(clickCategoryId) || clickCategoryId == null) {
          clickCategoryId = ""
        }
        val clickCategoryIds = new StringBuffer(clickCategoryId)

        if (userVisitAction.click_category_id != null && !clickCategoryIds.toString.contains(userVisitAction.click_category_id)) {
          clickCategoryId += (userVisitAction.click_category_id + ",")
        }

        val actionTime: Date = DateUtils.parseTime(userVisitAction.action_time)
        if (startTime.after(actionTime)) {
          startTime = actionTime
        }

        if (actionTime.after(endTime)) {
          endTime = actionTime
        }

        stepLength += 1

        val visitLength = (endTime.getTime - startTime.getTime) / 1000
        Constants.FIELD_SESSION_ID + "=" + session_id + "|" +
          Constants.FIELD_USER_ID + "=" + user_id + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|" +
          Constants.FIELD_END_TIME + "=" + DateUtils.formatTime(endTime)

      }, (aggrInfo1, aggrInfo2) => {
        //分区间聚合
        val user_id: String = StringUtils.getFieldFromConcatString(aggrInfo1, "\\|", Constants.FIELD_USER_ID)
        val session_id: String = StringUtils.getFieldFromConcatString(aggrInfo1, "\\|", Constants.FIELD_SESSION_ID)

        var searchKeywords1: String = StringUtils.getFieldFromConcatString(aggrInfo1, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
        var clickCategoryId1 = StringUtils.getFieldFromConcatString(aggrInfo1, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
        var visitLength1: Long = StringUtils.getFieldFromConcatString(aggrInfo1, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
        var stepLength1: Int = StringUtils.getFieldFromConcatString(aggrInfo1, "\\|", Constants.FIELD_STEP_LENGTH).toInt
        var startTime1: Date = DateUtils.parseTime(StringUtils.getFieldFromConcatString(aggrInfo1, "\\|", Constants.FIELD_START_TIME));
        var endTime1: Date = DateUtils.parseTime(StringUtils.getFieldFromConcatString(aggrInfo1, "\\|", Constants.FIELD_END_TIME));

        var searchKeywords2: String = StringUtils.getFieldFromConcatString(aggrInfo2, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
        var clickCategoryId2 = StringUtils.getFieldFromConcatString(aggrInfo2, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
        var visitLength2: Long = StringUtils.getFieldFromConcatString(aggrInfo2, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
        var stepLength2: Int = StringUtils.getFieldFromConcatString(aggrInfo2, "\\|", Constants.FIELD_STEP_LENGTH).toInt
        var startTime2: Date = DateUtils.parseTime(StringUtils.getFieldFromConcatString(aggrInfo2, "\\|", Constants.FIELD_START_TIME));
        var endTime2: Date = DateUtils.parseTime(StringUtils.getFieldFromConcatString(aggrInfo2, "\\|", Constants.FIELD_END_TIME));

        if (StringUtils.isEmpty(searchKeywords1) || searchKeywords1 == null) {
          searchKeywords1 = ""
        }
        val searchKeywords = new StringBuffer(searchKeywords1)
        if (StringUtils.isNotEmpty(searchKeywords2)) {
          for (skw: String <- searchKeywords2.split(",")) {
            if (StringUtils.isNotEmpty(skw) && !searchKeywords.toString.contains(skw)) {
              searchKeywords.append(skw)
            }
          }
        }

        if (StringUtils.isEmpty(clickCategoryId1) || clickCategoryId1 == null) {
          clickCategoryId1 = ""
        }
        val clickCategoryId = new StringBuffer(clickCategoryId1)
        if (StringUtils.isNotEmpty(clickCategoryId2)) {
          for (cci: String <- clickCategoryId2.split(",")) {
            if (StringUtils.isNotEmpty(cci.toString) && !clickCategoryId.toString.contains(cci)) {
              clickCategoryId.append(cci)
            }
          }
        }


        var startTime: Date = startTime1
        if (startTime1.after(startTime2)) {
          startTime = startTime2
        }

        var endTime: Date = endTime1
        if (endTime1.after(endTime2)) {
          endTime = endTime2
        }

        val stepLength: Long = stepLength1.toLong + stepLength2.toLong

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        Constants.FIELD_SESSION_ID + "=" + session_id + "|" +
          Constants.FIELD_USER_ID + "=" + user_id + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryId + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|" +
          Constants.FIELD_END_TIME + "=" + DateUtils.formatTime(endTime)

      }
    )

    val userId2AggrInfoRDD: RDD[(Long, String)] = session2AggrStr.map {
      case (session_id, aggrStr) => {
        val user_id: Long = StringUtils.getFieldFromConcatString(aggrStr, "\\|", Constants.FIELD_USER_ID).toLong
        (user_id, aggrStr)
      }
    }


    //获取user表
    val userId2userInfoRDD: RDD[(Long, UserInfo)] = getUserRDD(ss)

    //关联user表
    val sessionId2FullInfoRDD: RDD[(String, String)] = getSessonJoinUser(userId2AggrInfoRDD, userId2userInfoRDD)

    val accumulator: SessionAccumulator = new SessionAccumulator()

    ss.sparkContext.register(accumulator)
    val filerRDD: RDD[(String, String)] = getFilterFullInfoRDD(ss, sessionId2FullInfoRDD, taskParamsJsonObject, accumulator)


    filerRDD.foreach(println)

    val sessionMap: mutable.HashMap[String, Int] = accumulator.value

    getSessionRatio(ss, taskId, sessionMap)

    //计算比率


    //val value: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
    //value.collect().foreach(println)


    ss.stop()

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




  def getActionRDD(ss: SparkSession, taskParamsJsonObject: JSONObject): RDD[UserVisitAction] = {
    val startDate: String = taskParamsJsonObject.getString(Constants.PARAM_START_DATE)
    val endDate: String = taskParamsJsonObject.getString(Constants.PARAM_END_DATE)

    val sql = s"select * from user_visit_action where date>'${startDate}' and date<'${endDate}'"

    import ss.implicits._
    val actionRDD: RDD[UserVisitAction] = ss.sql(sql).as[UserVisitAction].rdd


    actionRDD
  }

}

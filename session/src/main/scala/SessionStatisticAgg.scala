import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object SessionStatisticAgg {
  /**
    * 获取聚合数据里面的聚合信息
    *
    * @param sparkSession
    * @param sessionId2GroupRDD
    */
  def getFullInfoData(sparkSession: SparkSession,
                      sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来，<userid,
    // partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
    val userId2AggrInfoRDD = sessionId2GroupRDD.map {
      case (sid, iterableAction) =>
        // session的起始和结束时间
        var startTime: Date = null
        var endTime: Date = null
        var userId = -1L

        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")
        // session的访问步长
        var stepLength = 0
        // 遍历session所有的访问行为
        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }
          // 计算session开始和结束时间
          val actionTime = DateUtils.parseTime(action.action_time)

          if (startTime == null || startTime.after(actionTime))
            startTime = actionTime

          if (endTime == null || endTime.before(actionTime))
            endTime = actionTime

          val searchKeyword = action.search_keyword
          val clickCategory = action.click_category_id
          // 实际上这里要对数据说明一下
          // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
          // 其实，只有搜索行为，是有searchKeyword字段的
          // 只有点击品类的行为，是有clickCategoryId字段的
          // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的
          // 我们决定是否将搜索词或点击品类id拼接到字符串中去
          // 首先要满足：不能是null值
          // 其次，之前的字符串中还没有搜索词或者点击品类id
          if (StringUtils.isNotEmpty(searchKeyword) &&
            !searchKeywords.toString.contains(searchKeyword))
            searchKeywords.append(searchKeyword + ",")

          if (clickCategory != -1L &&
            !clickCategories.toString.contains(clickCategory))
            clickCategories.append(clickCategory + ",")

          stepLength += 1
        }

        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sid + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)
    }
    // 查询所有用户数据，并映射成<userid,Row>的格式
    val sql = "select * from user_info"

    import sparkSession.implicits._
    // sparkSession.sql(sql): DateFrame DateSet[Row]
    // sparkSession.sql(sql).as[UserInfo]: DateSet[UserInfo]
    //  sparkSession.sql(sql).as[UserInfo].rdd: RDD[UserInfo]
    // sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item)): RDD[(userId, UserInfo)]
    val userInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))
    // 将session粒度聚合数据，与用户信息进行join，
    // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
    userId2AggrInfoRDD.join(userInfoRDD).map {
      case (userId, (aggrInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
    }
  }

  /**
    * 业务需求一：过滤session数据，并进行聚合统计
    * @param taskParam
    * @param sessionId2FullInfoRDD
    */
  def getFilteredData(taskParam: JSONObject, sessionId2FullInfoRDD: RDD[(String, String)]): Unit = {

    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if(startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if(filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    // 根据筛选参数进行过滤
    sessionId2FullInfoRDD.filter{
      case (sessionId, fullInfo) =>
        var success = true
        // 接着，依次按照筛选条件进行过滤
        // 按照年龄范围进行过滤（startAge、endAge）
        if(!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
          success = false
        // 按照职业范围进行过滤（professionals）
        // 互联网,IT,软件
        // 互联网
        if(!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS))
          success = false
        // 按照城市范围进行过滤（cities）
        // 北京,上海,广州,深圳
        // 成都
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES))
          success = false
        // 按照性别进行过滤
        // 男/女
        // 男，女
        if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX))
          success = false
        // 按照搜索词进行过滤
        // 我们的session可能搜索了 火锅,蛋糕,烧烤
        // 我们的筛选条件可能是 火锅,串串香,iphone手机
        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
        // 任何一个搜索词相当，即通过
        if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS))
          success = false
        // 按照点击品类id进行过滤
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS))
          success = false
        // 如果符合任务搜索需求
        if(success){

        }

        success
    }
  }

  def main(args: Array[String]): Unit = {
    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)
    // 任务的执行ID，用户唯一标示运行后的结果，用在MySQL数据库中
    val taskUUID = UUID.randomUUID().toString
    // 构建Spark上下文
    val sparkConf = new SparkConf().setAppName("SessionAnalyzer").setMaster("local[*]")
    // 创建Spark客户端
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    // 首先要从user_visit_action的Hive表中，查询出来指定日期范围内的行为数据
    val actionRDD = getActionRDD(sparkSession, taskParam)
    // 将用户行为信息转换为 K-V 结构
    val sessionid2actionRDD = actionRDD.map(item => (item.session_id, item))
    //转换成sessionId2GroupRDD: rdd[(sid, iterable(UserVisitAction))]
    val sessionid2GroupRDD = sessionid2actionRDD.groupByKey()
    // 将数据进行内存缓存
    sessionid2GroupRDD.persist(StorageLevel.MEMORY_ONLY)
    //测试1
    //    sessionid2GroupRDD.foreach(println(_))
    // 获取聚合数据里面的聚合信息
    val sessionId2FullInfoRDD = getFullInfoData(sparkSession, sessionid2GroupRDD)
    //测试2
    sessionId2FullInfoRDD.foreach(println(_))


    sparkSession.close()
  }

  /**
    * 根据日期获取对象的用户行为数据
    *
    * @param sparkSession
    * @param taskParam
    * @return
    */
  def getActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    import sparkSession.implicits._
    sparkSession.sql("select * from user_visit_action where date>='" + startDate
      + "' and date<='" + endDate + "'")
      .as[UserVisitAction].rdd
  }
}

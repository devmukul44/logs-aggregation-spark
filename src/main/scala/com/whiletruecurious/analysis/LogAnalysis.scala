package com.whiletruecurious.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object LogAnalysis {
  def main(args: Array[String]): Unit = {
    // SparkSession
    val sparkSession = SparkSession.builder.master("local").appName("log-com.whiletruecurious.analysis").enableHiveSupport().getOrCreate()
    // Logger
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    // Configurations
    val logsPath = "src/main/resources/logs"
    val sessionWindowRangeInHours = 4
    val sessionWindowRangeInDays = 20

    // Logs DataFrame
    val logsDF = getCsv(sparkSession, logsPath).withColumn("timestamp", col("timestamp").cast(LongType))
    // Session Id Generated DataFrame
    val sessionDF = generateSessionInRange(logsDF, sessionWindowRangeInHours)
    sessionDF.show()
    System.exit(0)

    // Count Unique Sessions in `sessionWindowRangeInDays`
    val sessionCountDF = countUniqueSessionInRange(sessionDF, sessionWindowRangeInDays)
    // Tag Engagement Level of User
    val tagDF = getUserTaggedDF(sessionCountDF)
    // Get the Count of each Tag
    val tagAggregationDF = getTagDistribution(tagDF)
    tagAggregationDF.show()
    // Persisting Output
//    writeDataFrame(tagDF, "src/main/resources/output")
    tagDF.show()
  }

  // Converts Hours to MilliSeconds
  val hrsInMillis = (hours:Int) => hours * 60 * 60 * 1000
  // Converts Days to MilliSeconds
  val daysInMillis = (days: Int) => days * 24 * 60 * 60 * 1000

  /** Generating Unique session Id for each Session
    *
    * @param df
    * @param rangeHours
    * @return
    */
  def generateSessionInRange(df: DataFrame, rangeHours: Int): DataFrame = {
    // Creating Aggregation Window on `user_id` and ascending order on `timestamp`
    val userWindow = Window.partitionBy(col("user_id")).orderBy("timestamp")
    // Using `lag` function to compare previous and current timestamp and checking if timestamp is greater than `rangeHours`
    // Note: This column can have set => {0, 1}, 0 for consecutive logs less than `rangeHours` and 1 otherwise
    val newSession =  (coalesce(col("timestamp") - lag(col("timestamp"), 1).over(userWindow), lit(0)) > hrsInMillis(rangeHours)).cast("bigint")
    // Note: Range of the following window is from `Window.unboundedPreceding` to `Window.currentRow`, because of orderBy operation on window
    // Generating Unique session Id for each session
    df.withColumn("session_id", sum(newSession).over(userWindow))
  }

  /** Calculate Unique `session_id` count in each Specified Range
    *
    * @param df
    * @param rangeDays
    * @return
    */
  def countUniqueSessionInRange(df: DataFrame, rangeDays: Int): DataFrame = {
    // Creating Aggregation Window on `user_id` and ascending order on `timestamp`
    // Note: Range of the following window is from -`rangeDays` to `Window.currentRow`
    val userWindow = Window.partitionBy(col("user_id")).orderBy("timestamp").rangeBetween(-daysInMillis(rangeDays), 0)
    // Calculating Unique `session_id` count in each window
    df.select(col("*"), size(collect_set(col("session_id")).over(userWindow)).as("session_count"))
  }

  /** Tagging with Engagement Level
    *
    * @param df
    * @return
    */
  def getUserTaggedDF(df: DataFrame): DataFrame = {
    val userTagUDF = udf{(sessionCount: Int) =>
      sessionCount match {
        case x if x >= 120 => "upu"
        case x if x >= 60 => "pu"
        case x if x >= 4 => "eu"
        case _ => "au"
      }
    }

    df.withColumn("engagement_tag", userTagUDF(col("session_count")))
  }

  /** Aggregates and Count Each Type of Tag in `engagement_tag` column
    *
    * @param df
    * @return
    */
  def getTagDistribution(df: DataFrame): DataFrame = df.groupBy("engagement_tag").count

  /** Returns DataFrame for csv file present in specified `filepath`
    *
    * Since Spark reader is used for reading, the index File can be present in HDFS / S3 or any distributed file system
    * supported by Spark.
    *
    * @param sparkSession
    * @param filepath
    * @return
    */
  def getCsv(sparkSession: SparkSession, filepath: String): DataFrame = {
    sparkSession.read.option("header","true").csv(filepath)
  }

  /** Writing Output DataFrame
    *
    * @param df
    * @param path
    */
  def writeDataFrame(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .csv(path)
  }
}

package com.whiletruecurious.generate

import java.sql.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/** Generate Randomised User Data
  *
  */
object LogsGenerator {
  def main(args: Array[String]): Unit = {
    // SparkSession
    val sparkSession = SparkSession.builder.master("local").appName("log-com.whiletruecurious.analysis").enableHiveSupport().getOrCreate()
    // Logger
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    // Configuration
    val startDate = "2018-03-01"
    val endDate = "2018-07-31"
    val numberOfUsers = 100
    val numberOfRecords = 1000
    val logsPath = "src/main/resources/logs"

    // Random Generated DataFrame
    val randomDataFrame = generateData(sparkSession, startDate, endDate, numberOfUsers, numberOfRecords)
    writeDataFrame(randomDataFrame, logsPath)
  }

  /** Generating User Random Data
    *
    * @param sparkSession
    * @return
    */
  def generateData(sparkSession: SparkSession, startDate: String, endDate: String, numberOfUsers: Int = 100, numberOfRecords: Int = 1000)
  : DataFrame = {
    import sparkSession.implicits._
    val startEpoch = Date.valueOf(startDate).getTime
    val endEpoch = Date.valueOf(endDate).getTime
    val epochDifference = endEpoch - startEpoch
    val r = new scala.util.Random

    // Creating Unique Users - Specified Number
    val userList = (1 to numberOfUsers).toList.map{u => "user-" + u}
    val dataTuple = userList.flatMap{ user =>
      // Generating Random Timestamp Between The Specified Range
      (1 to numberOfRecords).map{_ =>
        val randomTimestamp = startEpoch + (r.nextDouble() * epochDifference).toLong
        (user, randomTimestamp)
      }
    }
    // Creating User Data DataFrame
    val userDF = sparkSession.sparkContext.parallelize(dataTuple).toDF("user_id", "timestamp")

    userDF
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

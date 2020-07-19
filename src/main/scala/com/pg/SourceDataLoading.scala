package com.pg
import com.pg.utils.Constants
import com.pg.utils.Utility
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._

object SourceDataLoading {
  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder
        .master("local[*]")
        .appName("Sample project")
        .getOrCreate()
      spark.sparkContext.setLogLevel(Constants.ERROR)
      val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
      val s3Config = rootConfig.getConfig("s3_conf")
      val s3Bucket = s3Config.getString("s3_bucket")
      val SrcList = rootConfig.getStringList("SOURCE_DATA").toList
      spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
      spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))
      for(src <- SrcList) {
        val srcConfig = rootConfig.getConfig(src)
        src match {
          case "OL" =>
            val df = Utility
              .readFromSftp(spark, srcConfig.getConfig("sftp_conf"), srcConfig.getString("filename"))
              .withColumn("ins_ts", current_date())
            df.show()
            Utility.WriteToS3(df, s3Bucket, src)

          case "SB" =>
            val txnDf = Utility.readFromMysql(spark, srcConfig.getConfig("mysql_conf"), srcConfig.getString("table"))
              .withColumn("ins_ts", current_date())
            txnDf.show()
            Utility.WriteToS3(txnDf, s3Bucket, src)

          case "1CP" =>
            val cpDf = Utility.readFromS3(spark, srcConfig.getString("filename"))
              .withColumn("ins_ts", current_date())
            cpDf.show()
            Utility.WriteToS3(cpDf, s3Bucket, src)

        }
      }
      spark.stop()
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
      }
    }
  }
}

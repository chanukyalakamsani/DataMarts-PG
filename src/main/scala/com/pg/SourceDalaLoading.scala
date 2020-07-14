package com.pg
import com.pg.utils.{Constants,Utility}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import com.pg.IOperations1


object SourceDalaLoading {
  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder
        .master("local[*]")
        .appName("Sample project")
        .getOrCreate()
      spark.sparkContext.setLogLevel(Constants.ERROR)
      val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
      val sftpConfig = rootConfig.getConfig("sftp_conf")
      val s3Config = rootConfig.getConfig("s3_conf")
      spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
      spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))
      //val temp = new IOperations1(spark)
      val localpath = s"${sftpConfig.getString("directory")}/"
      val filename = "receipts_delta_GBR_14_10_2017.csv"
      val finalpath = "s3n://${s3Config.getString(\"s3_bucket\")}/test-datamart/OL/"
      val df = Utility.sftp(spark,sftpConfig,localpath+filename)
      df.show(false)
      Utility.WriteToS3(df,finalpath)

    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
      }
    }

  }

}

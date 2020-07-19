package com.pg

import com.pg.utils.{Constants, Utility}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConversions._

object TargetDataLoading {
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
      spark.udf.register("FN_UUID", () => java.util.UUID.randomUUID().toString())

      val tgtList = rootConfig.getStringList("DATAMART_TABLES").toList
      spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
      spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))
      for (tgt <- tgtList) {
        val tgtConfig = rootConfig.getConfig(tgt)
        tgt match {
          case "REGIS_DIM" =>
            val cpDf = Utility.readFromS3(spark, tgtConfig.getString("sourceFile"), fileType= "parquet")
            cpDf.show()
            cpDf.createOrReplaceTempView(tgtConfig.getString("sourceFile"))
            val regisDf = spark.sql(tgtConfig.getString("loadingQuery"))
            regisDf.show()
            regisDf.printSchema()
            val jdbcUrl = Constants.getRedshiftJdbcUrl(rootConfig.getConfig("redshift_conf"))
            //print(jdbcUrl)
            Utility.writeToRedshift(regisDf.coalesce(1), rootConfig.getConfig("redshift_conf"), s3Bucket, tgtConfig.getString("tableName"))
            println("Data is written to Redshift")
          case "CHILD_DIM" =>
            val cpDf = Utility.readFromS3(spark, tgtConfig.getString("sourceFile"), fileType= "parquet")
            cpDf.show()
            cpDf.createOrReplaceTempView(tgtConfig.getString("sourceFile"))
            val regisDf = spark.sql(tgtConfig.getString("loadingQuery"))
            regisDf.show()
            regisDf.printSchema()
            val jdbcUrl = Constants.getRedshiftJdbcUrl(rootConfig.getConfig("redshift_conf"))
            //print(jdbcUrl)
            Utility.writeToRedshift(regisDf.coalesce(1), rootConfig.getConfig("redshift_conf"), s3Bucket, tgtConfig.getString("tableName"))
            println("Data is written to Redshift")
          case "RTL_TXN_FACT" =>
            val sourceList = tgtConfig.getStringList("sourceFile").toList
            val jdbcUrl = Constants.getRedshiftJdbcUrl(rootConfig.getConfig("redshift_conf"))
           for (src <- sourceList) {
             val cpDf = Utility.readFromS3(spark, src, fileType= "parquet")
             cpDf.show()
             cpDf.createOrReplaceTempView(src)
           }
            val df = Utility.readToRedshift(spark, rootConfig.getConfig("redshift_conf"), s3Bucket, tgtConfig.getString("sourceTable"))
            df.createOrReplaceTempView("REGIS_DIM")
            val regisDF = spark.sql(tgtConfig.getString("loadingQuery"))
            //val jdbcUrl = Constants.getRedshiftJdbcUrl(rootConfig.getConfig("redshift_conf"))
            Utility.writeToRedshift(regisDF.coalesce(1), rootConfig.getConfig("redshift_conf"), s3Bucket, tgtConfig.getString("tableName"))
            println("Data is written to Redshift")
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

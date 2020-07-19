package com.pg.utils

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.current_date
import org.apache.spark.sql.SaveMode

import scala.reflect.io.Path

object Utility {
  def readcsv(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(path)
    return df
  }


  def readjson(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json(path)
    return df
  }


  def jointwotables(a: DataFrame, b: DataFrame, key: String, key1: String): DataFrame = {
    val joineddf = a.join(b, a(key) === b(key1), "inner")
    return joineddf.drop("id")
  }


  def readFromSftp(spark: SparkSession, sftpConfig: Config, path: String): DataFrame = {
    spark
      .read
      .format("com.springml.spark.sftp").
      option("host", sftpConfig.getString("hostname")).
      option("port", sftpConfig.getString("port")).
      option("username", sftpConfig.getString("username")).
      option("pem", sftpConfig.getString("pem")).
      option("fileType", "csv").
      option("delimiter", "|").
      load(s"${sftpConfig.getString("directory")}/$path")
  }

  def WriteToS3(df: DataFrame, s3Bucket: String, filename: String): Unit = {
    try {
      val df1 = df.withColumn("ins_ts", current_date())
      df1.show()
      df.write
        .partitionBy("ins_ts")
        .option("header", "true")
        .mode("overwrite")
        .parquet(s"s3n://$s3Bucket/PG-DataMart/$filename")
    }
  }


    def readFromMysql(spark: SparkSession, mysqlConfig: Config, tableName: String): DataFrame ={
      var jdbcParams = Map("url" -> getMysqlJdbcUrl(mysqlConfig),
        "lowerBound" -> "1",
        "upperBound" -> "100",
        "dbtable" -> (mysqlConfig.getString("database") + "." +tableName),
        "numPartitions" -> "2",
        "user" -> mysqlConfig.getString("username"),
        "password" -> mysqlConfig.getString("password")
      )
      spark
        .read.format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .options(jdbcParams)                                                  // options can pass map
        .load()
    }

    def getMysqlJdbcUrl(mysqlConfig:Config): String = {
      val host = mysqlConfig.getString("hostname")
      val port = mysqlConfig.getString("port")
      val database = mysqlConfig.getString("database")
      s"jdbc:mysql://$host:$port/$database?autoReconnect=true&useSSL=false"
    }

    def readfromredshift(spark:SparkSession,txtnDF:DataFrame,redshiftConfig: Config, s3Config:Config): DataFrame = {
      val jdbcUrl = Constants.getRedshiftJdbcUrl(redshiftConfig)
      val s3Bucket = s3Config.getString("s3_bucket")
      val txnDf = spark.read
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("tempdir", s"s3n://${s3Bucket}/temp")
        .option("forward_spark_s3_credentials", "true")
        .option("query", "select * from PUBLIC.TXN_FCT")
        //where merchant_id = 259
        //.option("dbtable", "PUBLIC.TXN_FCT")
        .load()
      return txnDf
    }

  def readFromS3(spark:SparkSession, fileName:String, delimiter:String = "|", fileType: String = "csv"): DataFrame = {
    if(fileType.equals("csv")) {
      spark.read
        .option("header","true")
        .option("delimiter", delimiter)
        .csv(s"s3n://chanukya-test/$fileName")
    } else {
      spark.read.parquet(s"s3n://chanukya-test/PG-DataMart/$fileName")
    }
  }

  def writeToRedshift(txtnDF:DataFrame, redshiftCOnf: Config, s3Bucket:String, tableName:String): Unit = {
    txtnDF.write
      .format("com.databricks.spark.redshift")
      .option("url", Constants.getRedshiftJdbcUrl(redshiftCOnf))
      .option("dbtable", s"${tableName}")
      .option("aws_iam_role", "arn:aws:iam::375635258862:role/myRedshiftRole")
      .option("tempdir", s"s3n://$s3Bucket/temp/")
      .mode(SaveMode.Overwrite)
      .save()
  }

  def readToRedshift(sparkSession: SparkSession, redshiftConf: Config, s3Bucket:String, tableName:String): DataFrame = {
    sparkSession.read
      .format("com.databricks.spark.redshift")
      .option("url", Constants.getRedshiftJdbcUrl(redshiftConf))
      .option("tempdir", s"s3n://${s3Bucket}/temp")
      .option("forward_spark_s3_credentials", "true")
      .option("dbtable", s"$tableName")
      .load()

  }
}

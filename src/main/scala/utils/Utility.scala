package utils

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.current_date

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


  def sftp(spark: SparkSession, sftpConfig: Config, path: String): DataFrame = {
    var olTxnDf = spark
      .read
      .format("com.springml.spark.sftp").
      option("host", sftpConfig.getString("hostname")).
      option("port", sftpConfig.getString("port")).
      option("username", sftpConfig.getString("username")).
      option("pem", sftpConfig.getString("pem")).
      option("fileType", "csv").
      option("delimiter", "|").
      load(path)
    return olTxnDf
  }

  def WriteToS3(df: DataFrame, s3_bukcet:String, filename:String): String = {
    try {
      val df1 = df.withColumn("ins_ts", current_date())
      df1.show()
      df.write
        .partitionBy("ins_ts")
        .option("header", "true")
        .mode("overwrite")
        .parquet(s"s3n://${s3_bucket}/PG-DataMart/${filename}/")
      return "Data is written to S3"
    }


  }
}

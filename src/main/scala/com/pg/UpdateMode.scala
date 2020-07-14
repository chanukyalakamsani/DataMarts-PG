package com.pg
import utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object UpdateMode {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "/")
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Streaming Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val dataPath = s"s3n://${s3Config.getString("s3_bucket")}/droplocation"

    val schema = StructType(
      StructField("city_code", StringType, true) ::
        StructField("city", StringType, true) ::
        StructField("major_category", StringType, true) ::
        StructField("minor_category", StringType, true) ::
        StructField("value", StringType, true) ::
        StructField("year", StringType, true) ::
        StructField("month", StringType, true) :: Nil)

    val fileStreamDF = sparkSession.readStream
      .option("header", "true")
      .schema(schema)
      .csv(dataPath)

    println("Is the stream ready? " + fileStreamDF.isStreaming)

    fileStreamDF.printSchema()

    val trimmedDF = fileStreamDF
      //      .filter("city = 'Southwark' and minor_category = 'Possession Of Drugs'")
      .select("city", "year", "month", "value")
      .withColumnRenamed("value","convictions")

    //val deltaMergePath = s"s3a://${s3Config.getString("s3_bucket")}/delta_merge_delta"
    val deltaTablePath = s"s3a://${s3Config.getString("s3_bucket")}/schema_enforcement_delta"


    val step = "overwrite"
    step match {
      case "overwrite" =>
        trimmedDF.printSchema()
        trimmedDF.show()
        println("Writing data,")
        trimmedDF
          .coalesce(1)
          .write
          .format("delta")
          .mode("append")
          .save(deltaTablePath)


      case "append" =>
        val deltaMergeDf  = DeltaTable.forPath(sparkSession, deltaTablePath)
        deltaMergeDf.alias("delta_merge")
          .merge(trimmedDF.alias("updates"),"delta_merge.city = updates.city")
          .whenMatched()
          .updateExpr(Map("year" -> "updates.year", "month" -> "updates.month", "convictions" -> "updates.convictions"))
          .whenNotMatched()
          .insertExpr(Map("city"->"updates.city","year" -> "updates.year", "month" -> "updates.month", "convictions" -> "updates.convictions"))
          .execute()

        println("Reading data,")
        DeltaTable.forPath(sparkSession, deltaTablePath).toDF.show()
    }



    val query = trimmedDF.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 30)
      .start()
      .awaitTermination()

  }

}

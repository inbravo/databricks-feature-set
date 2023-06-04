package com.inbravo.delta

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ LongType, StringType, StructField, StructType }

/**
 *
 */
object SparkDeltaTest {

  /* Schema of Person */
  val personSchema = StructType(Seq(StructField("name", StringType, nullable = true), StructField("age", LongType, nullable = true)))

  /* A list of persons */
  val persons = Seq(Row("Amit", 20L), Row("Anuj", 30L), Row("Aniket", 25L), Row("Arihant", 45L), Row("Avirat", 75L), Row("Avishi", 35L))

  /* Case classes to represent a business object  */
  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    /* To avoid 'winutils.exe' error on windows */
    if (System.getProperty("os.name").toLowerCase.contains("window")) {
      System.setProperty("hadoop.home.dir", "D:/open-source/hadoop-3.3.0");
    }

    /* Create new local spark session with Single threads per Core */
    val sparkSession = SparkSession.builder.config(new SparkConf().setAppName("SparkDFTest").setMaster("local[*]"))
      .getOrCreate

    /* Change log level to avoid lots of log */
    sparkSession.sparkContext.setLogLevel("ERROR")

    /* Print the spark version */
    println("Spark version: " + sparkSession.sparkContext.version)

    /* Run all examples */
    // readAsDelta(sparkSession)
    createParquetData(sparkSession)
    convertParquetToDelta(sparkSession)
  }

  /**
   * Schema based DataFrame operations
   */
  private def saveAsDelta(sparkSession: SparkSession): Unit = {

    /* Create persons RDD */
    val personsRDD = sparkSession.sparkContext.parallelize(persons)

    /* Example 1 : Create empty data frame using Schema only */
    val emptyDataFrame: DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], personSchema)

    println("Empty Persons Data Frame: ")
    emptyDataFrame.toDF.show
    println("-----------------------------------------------------")

    /*  Example 2 : Create persons data frame using RDD and Schema */
    val personsDataFrame: DataFrame = sparkSession.createDataFrame(personsRDD, personSchema)

    println("Persons Data Frame with Data: ")
    personsDataFrame.toDF.show
    println("-----------------------------------------------------")

    /* Save people data frame in delta format */
    personsDataFrame.write.format("delta").mode("overwrite").save("data/output/people.delta")
  }

  /**
   * Schema based DataFrame operations
   */
  private def readAsDelta(sparkSession: SparkSession): Unit = {

    /* Read people data frame in delta format */
    val personsDataFrame: DataFrame = sparkSession.read.format("delta").load("data/output/people.delta")

    println("Persons Data Frame with Data: ")
    personsDataFrame.toDF.show
    println("-----------------------------------------------------")
  }

  private def createParquetData(sparkSession: SparkSession): Unit = {

    /* This import is needed to use the $-notation, for implicit conversions like converting RDDs to DataFrames */
    import org.apache.commons.io.FileUtils
    import sparkSession.implicits._;

    /* Create an RDD of Person objects from a text file, convert it to a DataFrame */
    val personDF = sparkSession.sparkContext.textFile("src/main/resources/people.txt").map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toInt)).toDF

    try {

      /* Delete the Parquet directories */
      FileUtils.deleteDirectory(new java.io.File("data/output/people.parquet"));
    } catch {
      case e: Exception => println("Exception in deleting file" + e)
    }

    /* DataFrames can be saved as 'PARQUET' files, maintaining the schema information */
    personDF.write.parquet("data/output/people.parquet")
  }

  /**
   *
   */
  private def convertParquetToDelta(sparkSession: SparkSession): Unit = {

    /* This import is needed to use the DeltaTable */
    import io.delta.tables._;

    /* Convert to delta format */
    DeltaTable.convertToDelta(sparkSession, "parquet.`data/output/people.parquet`")
  }
}
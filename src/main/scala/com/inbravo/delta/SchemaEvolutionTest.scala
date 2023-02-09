package com.inbravo.delta

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ LongType, StringType, StructField, StructType }

/**
 *
 */
object SchemaEvolutionTest {

  /* Schema of Person */
  val personSchema = StructType(Seq(StructField("name", StringType, nullable = true), StructField("age", LongType, nullable = true)))
  
  /* A list of persons */
  val persons = Seq(Row("Amit", 20L), Row("Anuj", 30L), Row("Aniket", 25L), Row("Arihant", 45L), Row("Avirat", 75L), Row("Avishi", 35L))

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
    saveAsDelta(sparkSession)
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
    personsDataFrame.write.format("delta").mode("overwrite").save("people.delta")    
  }
}
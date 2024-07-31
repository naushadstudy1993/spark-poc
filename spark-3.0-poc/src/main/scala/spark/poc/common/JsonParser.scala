package spark.poc.common

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object JsonParser {

  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C://Users//naush//Documents//Professional//Workspace//Projects//Spark//hadoop//bin");
    System.load("C://Users//naush//Documents//Professional//Workspace//Projects//Spark//hadoop//bin//hadoop.dll");
    spark.sparkContext.setLogLevel("ERROR")

    //format_1()
    format_2()

  }

  def format_1(): Unit = {

    var input_df = spark.read.option("multiline", true).json("C://Users//naush//Documents//Professional//Workspace//Projects//Spark//Spark-3//Demo//Demo_Proj//data//input//json_data//")
    input_df.show(false)
    input_df.printSchema

    input_df = input_df.withColumn("source_explod", explode(array(col("source.*"))))
    input_df.show(false)
    input_df.printSchema

    var geo_type = StructType(
      Seq(
        StructField("lat", DoubleType, true),
        StructField("long", DoubleType, true)))

    var sensor_type = StructType(
      Seq(
        StructField("c02_level", LongType, true),
        StructField("description", StringType, true),
        StructField("geo", geo_type, true),
        StructField("id", LongType, true),
        StructField("ip", StringType, true),
        StructField("temp", LongType, true)))
        
    var s = StructType(
        Seq(
            StructField("sensor-igauge", sensor_type, true),
            StructField("sensor-ipad", sensor_type, true),
            StructField("sensor-inest", sensor_type, true),
            StructField("sensor-istick", sensor_type, true),
            
            )
        )

    var structType = StructType(
      Seq(
        StructField("dc_id", StringType, true),
        StructField("source", s, true)))
        
    /*var input_df1 = spark.read.schema(structType).option("multiline", true).json("C://Users//naush//Documents//Professional//Workspace//Projects//Spark//Spark-3//Demo//Demo_Proj//data//input//json_data//")  
    input_df1.show(false)
    input_df1.printSchema()*/
    
    var input_df1 = spark.read.schema(structType).option("multiline", true).json("C://Users//naush//Documents//Professional//Workspace//Projects//Spark//Spark-3//Demo//Demo_Proj//data//input//json_data//")  
    input_df1 = input_df1.withColumn("source_flat",col("source"))
    
    
    
    
  }
  
  def format_2() : Unit = {
      var input_df = spark.read.option("multiline", true).json("C://Users//naush//Documents//Professional//Workspace//Projects//Spark//Spark-3//Demo//Demo_Proj//data//input//json_data//")  
     // input_df = input_df.select(explode(array(col("source.*"))))//.select(col("col.*"))
      //input_df = input_df.select(col("source.*"))
      input_df.show(false)
      input_df.printSchema()
  }

}

/*

|-- sensor-igauge: struct (nullable = true)
 |    |    |-- c02_level: long (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- geo: struct (nullable = true)
 |    |    |    |-- lat: double (nullable = true)
 |    |    |    |-- long: double (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- ip: string (nullable = true)
 |    |    |-- temp: long (nullable = true)

*/
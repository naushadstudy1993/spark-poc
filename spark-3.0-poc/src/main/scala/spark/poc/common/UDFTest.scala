package spark.poc.common

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object UDFTest {
  
  var spark = SparkSession.builder.master("local").getOrCreate( )
  
  def main(args : Array[String]) : Unit = {
    var obj = new UDFClass(1)
   obj.m1(spark)
  }
  
  def m1() : Unit = {
    var records = Seq(
        List("Naushad", 1000),
        List("Naaz", 2000)
        )
        
    var structType = StructType(
        Seq(
            StructField("name", StringType, true),
            StructField("salary", IntegerType, true)
            )
        ) 
        
    var rdd = spark.sparkContext.parallelize(records).map(r => Row.fromSeq(r))
    var df = spark.createDataFrame(rdd, structType)
    df.show
    var udf_f1_reg = spark.udf.register("udf_f1", udf_f1)
   
    df =df.withColumn("incr_sal", udf_f1_reg(col("salary")))
    df.show
 }
  
  def udf_f1 = (sal : Int) => {
    sal*2
  }
}
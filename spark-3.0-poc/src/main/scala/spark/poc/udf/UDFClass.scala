package spark.poc.udf

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.Seq
import scala.reflect.api.materializeTypeTag

class UDFClass(n : Int)  extends Serializable{
  
   def m1(spark : SparkSession) : Unit = {
     
     
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
    
    var upperString_UDF = spark.udf.register("upperString", upperString)
    
    df.withColumn("name_upper", upperString_UDF(col("name"))).show
    
  
    
   }
  
  def udf_f1 = (sal : Int) => {
    sal*2
  }
  
  def upperString = (name : String) => {
    name.toUpperCase()
  }
  
}

package spark.poc.functions

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import scala.collection.Seq



case class Employee(id: Int, name: String, dept: String, salary: Int)

object WindowFunction {

  var spark: SparkSession = SparkSession.builder.master("local").appName("spark-local").getOrCreate()
 

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
     
    var input_df = dataLoad
    
    /***Ranking Functions RANK | DENSE_RANK | PERCENT_RANK | NTILE | ROW_NUMBER**/
    
    //allRankFunc(input_df)
    //allRankFuncSQL(input_df)
    
    //max_min(input_df)
    leetcode_197()  
  }

  def dataLoad(): DataFrame = {
    val records = Seq(
 List(1001, "Satılmış", "İdari", 4000),
 List(1002, "Özge", "Personel", 3000),
 List(1003, "Hüsnü", "Bilgi Sistemleri", 4000),
 List(1004, "Menşure", "Muhasebe", 6500),
 List(1005, "Doruk", "Personel", 3000),
 List(1006, "Şilan", "Muhasebe", 5000),
 List(1007, "Baran", "Personel", 7000),
 List(1008, "Ülkü", "İdari", 4000),
 List(1009, "Cüneyt", "Bilgi Sistemleri", 6500),
 List(1010, "Gülşen", "Bilgi Sistemleri", 7000),
 List(1011, "Melih", "Bilgi Sistemleri", 8000),
 List(1012, "Gülbahar", "Bilgi Sistemleri", 10000),
 List(1013, "Tuna", "İdari", 2000),
 List(1014, "Raşel", "Personel", 3000),
 List(1015, "Şahabettin", "Bilgi Sistemleri", 4500),
 List(1016, "Elmas", "Muhasebe", 6500),
 List(1017, "Ahmet Hamdi", "Personel", 3500),            
 List(1018, "Leyla", "Muhasebe", 5500),
 List(1019, "Cuma", "Personel", 8000),
 List(1020, "Yelda", "İdari", 5000),
 List(1021, "Rojda", "Bilgi Sistemleri", 6000),
 List(1022, "İbrahim", "Bilgi Sistemleri", 8000),
 List(1023, "Davut", "Bilgi Sistemleri", 8000),
 List(1024, "Arzu", "Bilgi Sistemleri", 11000)
)
    
     var schema = StructType(Seq( 
       StructField("id", IntegerType, false),
       StructField("name", StringType, false),
       StructField("dept", StringType, false),
       StructField("salary", IntegerType, false),
     ))
     
     var rdd = spark.sparkContext.parallelize(records).map(Row.fromSeq)
     var df = spark.createDataFrame(rdd, schema)
     df  
 }
 
  def allRankFunc(input_df : DataFrame): Unit = {
    input_df.withColumn("row_number", functions.row_number().over(Window.partitionBy(input_df.col("dept")).orderBy("salary") ) )
      .withColumn("rank", functions.rank().over(Window.partitionBy(input_df.col("dept")).orderBy("salary") ) )
      .withColumn("dense_rank", functions.dense_rank.over(Window.partitionBy(input_df.col("dept")).orderBy(input_df.col("salary"))))
      .withColumn("lag", functions.lag(input_df.col("salary"), 1, 0 ).over(Window.partitionBy(input_df.col("dept")).orderBy(input_df.col("salary") ) ))
      .withColumn("lead", functions.lead(input_df.col("salary"), 1, 0 ).over(Window.partitionBy(input_df.col("dept")).orderBy(input_df.col("salary")) ) )
    .show
  }
  
  def allRankFuncSQL(input_df : DataFrame): Unit = {
    input_df.createOrReplaceTempView("emp")
    
    spark.sql("select emp.*, row_number() OVER(partition by dept order by salary) as rn, rank() over(partition by dept order by salary) as rk from emp ").show
  }
  
  def max_min(input_df : DataFrame) : Unit = {
    input_df
    .withColumn("max", functions.max(col("salary")).over(Window.partitionBy(col("dept"))) )
    .withColumn("min", functions.min(col("salary")).over(Window.partitionBy(col("dept"))) )
    .show
    
  }
  
  
  def leetcode_197() : Unit ={
    spark.conf.set("spark.sql.crossJoin.enabled", true)
    var record = Seq(Row(1,"2015-01-01",10),Row(2,"2015-01-02",25), Row(3,"2015-01-03",20), Row(4,"2015-01-04",30))
    
    var schema = StructType(Seq(
      StructField("id", IntegerType,false),
      StructField("date", StringType, false ),
      StructField("temperature", IntegerType, false)
    ))
    
    var rdd = spark.sparkContext.parallelize(record)
    var df =spark.createDataFrame(rdd, schema)
    df.show
    
    var join_df = df.as("t1").join(df.as("t2"))
    join_df.show
    
    
    df.as("t1").join(df.as("t2"), col("t1.id").equalTo(col("t2.id")+lit(1)) ).show
    
    df.as("t1").join(df.as("t2"), col("t1.id").equalTo(col("t2.id")+lit(1)).and(col("t1.temperature").gt(col("t2.temperature")) ) ).show
  }

}
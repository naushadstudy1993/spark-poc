package spark.poc.sql.question

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.sql.Date
import scala.collection.Seq


/** Below Question exercise link
 *  https://www.edureka.co/blog/interview-questions/sql-query-interview-questions  
 *  
 *  
1. Write a query to fetch the EmpFname from the EmployeeInfo table in the upper case and use the ALIAS name as EmpName.
2. Write a query to fetch the number of employees working in the department ‘HR’.
3. Write a query to get the current date.
4. Write a query to retrieve the first four characters of  EmpLname from the EmployeeInfo table.
5. Write a query to fetch only the place name(string before brackets) from the Address column of EmployeeInfo table.
7. Write q query to find all the employees whose salary is between 50000 to 100000.
8. Write a query to find the names of employees that begin with ‘S’
9. Write a query to fetch top N records.
10. Write a query to retrieve the EmpFname and EmpLname in a single column as “FullName”. The first name and the last name must be separated with space.

 * */

object SparkTest {
  
  var spark : SparkSession = null
  
   val input_base_path : String = "C://Users//naush//Documents//Professional//Workspace//Projects//Spark//Spark-3//Demo//Demo_Proj//data//input//employee//"
   val employee_info : String = input_base_path + "employee_info.csv"
   val employee_position : String = input_base_path + "employee_position.csv"
  
  def main(args : Array[String]): Unit ={
    println("Main Method")
    
    System.setProperty ("hadoop.home.dir", "C://Users//naush//Documents//Professional//Workspace//Projects//hadoop//bin" ); 
    System.load ("C://Users//naush//Documents//Professional//Workspace//Projects//hadoop//bin//hadoop.dll");
    
    this.spark = SparkSession.builder.master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    //q2
    //q4
    //q5
    //q7
    //q8
    //q9(2)
    //q10
    //q11
    //q14
    //q20
    //q23
    q24
    
    
   }
   
   def q2() : Unit = {
     var emp_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_info)
     var pos_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_position)
     
     var emp_fil_df = emp_df.filter(col("department").equalTo("HR"))
     val emp_count : Long = emp_fil_df.count
     println(emp_count)
   }
   
   def q4() : Unit = {
     var emp_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_info)
     
     emp_df.show
     
     emp_df.withColumn("short_name", col("emplname").substr(0, 4))
     .select(col("short_name"))
     .show
   }
   
   def q5() : Unit = {
     var emp_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_info)
     emp_df.show
     
     var pos : Column = functions.instr(col("address"), "(" )
     
     emp_df.withColumn("address_1", functions.substring_index(col("address"), "(", 1))
     .select(col("address_1"))
     .show
   }
   
   def q7() : Unit = {
     var emp_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_info)
     var pos_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_position)
     
     var emp_pos_df = emp_df.join(pos_df, emp_df.col("empid").equalTo(pos_df.col("empid")))
     var final_df = emp_pos_df.filter(col("salary").between(lit(50000), lit(100000)))
     final_df.show
   }
   
   def q8() : Unit ={
     var emp_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_info)
     var final_df = emp_df.filter(functions.substring(col("empfname"), 0, 1).equalTo("S"))
     final_df.show
     
   }
   
   def q9(n : Int) : Unit = {
     var emp_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_info)
     var pos_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_position)
     
     var emp_pos_df = emp_df.join(pos_df, 
         emp_df.col("empid").equalTo(pos_df.col("empid")) 
         )
     
     /*var windowSpec : WindowSpec= Window.orderBy(col("salary").desc)
     var final_df = emp_pos_df.withColumn("row_num", functions.row_number().over(windowSpec))
     .filter(col("row_num").equalTo(lit(n)))*/
         
     var final_df = emp_pos_df.orderBy(col("salary").desc).limit(n)    
     
     final_df.show
     
   }
  
   def temp() : Unit = {
         var csv_df : Dataset[Row]  = spark.read.format("csv").option("header", true).option("InferSchema", true).load(input_base_path)
    csv_df.show
    
    csv_df.filter(col("id").geq(102)).show
    
    csv_df.groupBy(col("id"))
    .agg(sum(col("sal"))).show
   }
   
   /**Q10. Write a query to retrieve the EmpFname and EmpLname in a single column as “FullName”. The first name and the last name must be separated with space. */
   def q10() : Unit = {
     var emp_df : Dataset[Row] = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_info)
     val final_df = emp_df.withColumn("fname_lname", functions.concat(col("empfname"), lit(" "),col("emplname")))
     final_df.show
   }
   
   /**Q11. Write a query find number of employees whose DOB is between 02/05/1970 to 31/12/1975 and are grouped according to gender**/
   def q11() : Unit = {
     var emp_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_info)
     var pos_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_position)
     
     var emp_pos_df = emp_df.join(pos_df, emp_df.col("empid").equalTo(pos_df.col("empid"))).drop(pos_df.col("empId"))
     
     emp_pos_df = emp_pos_df.withColumn("DOB", functions.to_date(col("DOB"), "dd/mm/yyyy"))
     
     var dateFormat = new SimpleDateFormat("dd/mm/yyyy")
     var sDate = new Date(dateFormat.parse("02/05/1970").getTime)
     var eDate = new Date(dateFormat.parse("31/12/1978").getTime)
     
     var emp_filter_df = emp_pos_df.filter(col("DOB").between(sDate, eDate))
     var final_df = emp_filter_df.groupBy(col("gender")).agg(functions.count(col("empId")))
     final_df.show
   }
   
   /**Q14. Write a query to fetch details of all employees excluding the employees with first names, “Sanjay” and “Sonia” from the EmployeeInfo table.**/
   def q14() : Unit = {
     var emp_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_info)
      var list :Seq[String] = Seq("Sanjay", "Sonia")      
     //var final_df = emp_df.filter(col("empfname").notEqual("Sanjay").and(col("empfname").notEqual("Sonia")))
     var final_df = emp_df.filter(! col("empfname").isin(list : _*))
     
     final_df.show
   }

  /**Q20 Write a query to retrieve two minimum and maximum salaries from the EmployeePosition table..**/
  def q20(): Unit = {
    var emp_df: Dataset[Row] = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_info)
    var pos_df: Dataset[Row] = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_position)

    var emp_pos_df = emp_df.join(pos_df, emp_df.col("empId").equalTo(pos_df.col("empId"))).drop(pos_df.col("empid"))

    var minWindowSpec: org.apache.spark.sql.expressions.WindowSpec = org.apache.spark.sql.expressions.Window.orderBy(col("salary"))
    var maxWindowSpec: org.apache.spark.sql.expressions.WindowSpec = org.apache.spark.sql.expressions.Window.orderBy(col("salary").desc)

    var emp_sort_df = emp_pos_df.withColumn("minSalary", functions.row_number().over(minWindowSpec))
      .withColumn("maxSalary", functions.row_number().over(maxWindowSpec))

    emp_sort_df.show
    
    var final_df = emp_sort_df.filter(col("minSalary").leq(2).or(col("maxSalary").leq(2)))
    final_df.show
  }
  
  /** Q23. Write a query to retrieve the list of employees working in the same department.**/  
  def q23() : Unit = {
    var emp_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_info)
    var emp2_df = emp_df
    emp_df.show
   // var final_df = emp_df.as("emp1").join(emp_df.as("emp2"), col("emp1.Department").equalTo("emp2.Department").and(col("emp1.empId").notEqual(col("emp2.Department")))).select("emp1.*")
    var final_df = emp_df.join(emp2_df, emp_df.col("Department").equalTo(emp2_df.col("Department")).and(emp_df.col("empId").notEqual(emp2_df.col("empId"))))
    final_df.show
  }
  
  /**Q24. Write a query to retrieve the last 3 records from the EmployeeInfo table**/
  def q24() : Unit = {
    var emp_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(employee_info)
    emp_df.createOrReplaceTempView("EmployeeInfo")
    
    //spark.sql("SELECT * FROM EmployeeInfo WHERE EmpID <=3 UNION SELECT * FROM (SELECT * FROM EmployeeInfo E ORDER BY E.EmpID DESC) AS E1 WHERE E1.EmpID <=3;").show
    spark.sql("SELECT * FROM EmployeeInfo WHERE EmpID <=3 UNION SELECT * FROM (SELECT * FROM EmployeeInfo E ORDER BY E.EmpID DESC) AS E1 WHERE E1.EmpID <=3;").show
  }
  
  
}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}

/**
  * Created by markmo on 31/01/2016.
  */
object Setup {

  val conf = new SparkConf().setAppName("sparktemplate").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val delta = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs://localhost:9000/base/Customer_Demographics_Delta.csv")

  val deltaNames = delta.schema.fieldNames.toList

  val deltaFormatted = delta
    .select(col("cust_id").cast(StringType).as("cust_id") :: (updatesNames diff List("cust_id")).map(name => col(name).cast(LongType).as(name)): _*)

  println("###1")
  println(deltaFormatted.printSchema())

  deltaFormatted.write.parquet("hdfs://localhost:9000/base/Customer_Demographics_Delta.parquet")

  val updates = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs://localhost:9000/base/Customer_Demographics_Delta_Updates.csv")

  val updatesNames = updates.schema.fieldNames.toList

  val updatesFormatted = updates
    .select(col("cust_id").cast(StringType).as("cust_id") :: (updatesNames diff List("cust_id")).map(name => col(name).cast(LongType).as(name)): _*)

  println("###2")
  println(updatesFormatted.printSchema())

  updatesFormatted.write.parquet("hdfs://localhost:9000/base/Customer_Demographics_Delta_Updates.parquet")

}

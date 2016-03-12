import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by markmo on 31/01/2016.
  */
object Setup extends App {

  val BASE_URI = "hdfs://localhost:9000"
  val LAYER_RAW = "base"
  val LAYER_ACQUISITION = "acquisition"

  val conf = new SparkConf().setAppName("sparktemplate").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val delta = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s"$BASE_URI/$LAYER_RAW/Customer_Demographics_Delta.csv")

  val deltaNames = delta.schema.fieldNames.toList

  val deltaFormatted = delta
    .select(col("cust_id").cast(StringType).as("cust_id") :: (deltaNames diff List("cust_id")).map(name => col(name).cast(LongType).as(name)): _*)

  deltaFormatted.write.parquet(s"$BASE_URI/$LAYER_RAW/Customer_Demographics_Delta.parquet")

  val updates = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s"$BASE_URI/$LAYER_RAW/Customer_Demographics_Delta_Updates.csv")

  val updatesNames = updates.schema.fieldNames.toList

  val updatesFormatted = updates
    .select(col("cust_id").cast(StringType).as("cust_id") :: (updatesNames diff List("cust_id")).map(name => col(name).cast(LongType).as(name)): _*)

  updatesFormatted.write.parquet(s"$BASE_URI/$LAYER_RAW/Customer_Demographics_Delta_Updates.parquet")

  val emailMappings = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s"$BASE_URI/$LAYER_RAW/email_mappings.csv")

  emailMappings.write.parquet(s"$BASE_URI/$LAYER_RAW/email_mappings.parquet")

  val fnnMappings = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s"$BASE_URI/$LAYER_RAW/fnn_mappings.csv")

  fnnMappings.write.parquet(s"$BASE_URI/$LAYER_RAW/fnn_mappings.parquet")

}

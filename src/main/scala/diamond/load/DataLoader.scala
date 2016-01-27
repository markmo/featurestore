package diamond.load

import diamond.utility.udfs._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by markmo on 23/01/2016.
  */
trait DataLoader {

  val META_ENTITY_ID = "entity_id"
  val META_START_TIME = "start_time"
  val META_END_TIME = "end_time"
  val META_PROCESS_DATE = "process_date"
  val META_OP = "op"
  val META_VERSION = "version"
  val META_HASHED_VALUE = "hashed_value"
  val META_OPEN_END_DATE_VALUE = "9999-12-31"

  val BASE_URI = "hdfs://localhost:9000"

  val customerHubSchema = StructType(
    StructField("entity_id", StringType) ::
    StructField("customer_id", StringType) ::
    StructField("customer_id_type", StringType) ::
    StructField("process_time", StringType) :: Nil
  )

  def loadSatellite(df: DataFrame,
                    isDelta: Boolean,
                    tableName: String,
                    idField: String,
                    idType: String,
                    partitionKeys: Option[List[String]] = None,
                    newNames: Map[String, String] = Map())

  def hashRows(df: DataFrame, names: List[String]) =
    df.withColumn("hashed_value", fastHashUDF(concat(names.map(col): _*)))

  /*
  def hashRows(df: DataFrame, names: List[String]) =
    df.map { row =>
      val value = names.foldLeft("")((a, name) => a + row.getAs[String](name))
      Row(row.getAs[String]("entityId"), hashKey(value))
    }
   */

  //Parquet writes columns out of order (compared to the schema)
  //https://issues.apache.org/jira/browse/PARQUET-188
  //Fixed in 1.6.0

  def registerCustomers(df: DataFrame, idField: String, idType: String)

}

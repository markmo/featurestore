package diamond.load

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  *
  *
  * Created by markmo on 23/01/2016.
  */
trait DataLoader {

  val META_ENTITY_ID = "entity_id"
  val META_START_TIME = "start_time"
  val META_END_TIME = "end_time"
  val META_PROCESS_DATE = "process_date"
  val META_RECTYPE = "rectype"
  val META_VERSION = "version"
  val META_HASHED_VALUE = "hashed_value"
  val META_OPEN_END_DATE_VALUE = "9999-12-31"

  val RECTYPE_INSERT = "I"
  val RECTYPE_UPDATE = "U"
  val RECTYPE_DELETE = "D"

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
                    deleteIndicatorField: Option[(String, Any)] = None,
                    partitionKeys: Option[List[String]] = None,
                    newNames: Map[String, String] = Map(),
                    overwrite: Boolean = false,
                    writeChangeTables: Boolean = false)

  def registerCustomers(df: DataFrame, idField: String, idType: String)

}

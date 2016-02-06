package diamond.load

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * A Data Vault 2.0 like process is being employed. There are three types of tables:
  * * Hubs - contain the mapping of natural keys to hashed keys. This is done partly
  *          for convenience and partly for privacy.
  * * Links - contain the many-to-many mapping of hashed keys between entities, e.g.
  *           between Customer and Account (Service).
  * * Satellites - contain data attributes of interest against the hashed key
  *
  * The benefits of this approach include:
  * * Fast loading. With hashing, all tables can be loaded in parallel. A step to
  *   first lookup a surrogate key is not required. Satellite tables map 1:1 with
  *   source tables so there are no unnecessary joins. The model is optimised for
  *   load not necessarily query. However, in the analytic asset, feature engineering
  *   will select only the data required. The model is not intended to serve
  *   general query access.
  * * Privacy. Keys and PII data can be separated from non-sensitive data for analysis.
  *
  * The standard metadata for a Satellite table includes:
  * * entity_id - hashed key consisting of the id and id type (as there is the
  *   possibility of key overlap across different types of id)
  * * start_time - effective system start time
  * * end_time - effective system end time
  * * process_date - the date on which the file/table is processed
  * * rectype - the type of write operation (I)nsert, (U)pdate, (D)elete
  * * version - an incrementing integer to indicate the record version. Highest
  *             is latest.
  * * hashed_value - the hash of all the non-key attributes (excluding this metadata)
  *
  *
  * Created by markmo on 23/01/2016.
  */
trait DataLoader extends Serializable {

  val META_ENTITY_ID = "entity_id"
  val META_START_TIME = "start_time"
  val META_END_TIME = "end_time"
  val META_SOURCE = "source"
  val META_PROCESS_TYPE = "process_type"
  val META_PROCESS_ID = "process_id"
  val META_PROCESS_DATE = "process_date"
  val META_USER_ID = "user_id"
  val META_HASHED_VALUE = "hashed_value"
  val META_RECTYPE = "rectype"
  val META_VERSION = "version"
  val META_OPEN_END_DATE_VALUE = "9999-12-31"
  val META_VALID_START_TIME_FIELD = "valid_start_time_field"
  val META_VALID_END_TIME_FIELD = "valid_end_time_field"
  val META_VALID_START_TIME = "valid_start_time"
  val META_VALID_END_TIME = "valid_end_time"
  val META_DELETE_INDICATOR_FIELD = "delete_indicator_field"

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

  /**
    *
    * @param df DataFrame the DataFrame of the file/table to load into the Satellite table
    * @param isDelta Boolean to indicate whether the file contains deltas or a full refresh
    * @param tableName String name of the Satellite table
    * @param idFields Seq[String] name of the primary key field
    * @param idType String type of identifier, e.g. Siebel Customer Number, FNN
    * @param source String source system name
    * @param processType String type of process, e.g. "Load Delta", "Load History Full"
    * @param processId String unique process id
    * @param userId String user or system account of process
    * @param validStartTimeField Option[(String, Any)] a tuple with the first value the
    *                            name of the field that contains the valid (business) start
    *                            time and the second value the time format
    * @param validEndTimeField Option[(String, Any)] a tuple with the first value the
    *                          name of the field that contains the valid (business) end
    *                          time and the second value the time format
    * @param deleteIndicatorField Option[(String, Any)] a tuple with the first value the
    *                             name of the field that indicates a record has been deleted
    *                             and the second value the value set if the record has been
    *                             marked for deletion
    * @param partitionKeys Option[List[String]] an optional list of fields to partition on
    * @param newNames Map[String, String] a dictionary of new column names mapped to the
    *                 old names
    * @param overwrite Boolean to indicate whether changes should overwrite the Satellite
    *                  table and therefore existing records can be updated, e.g. setting
    *                  the `end_time`
    * @param writeChangeTables Boolean to indicate whether change table should be written.
    *                          Separate change tables for new records, updated records,
    *                          and deleted records are written for processing purposes.
    */
  def loadSatellite(df: DataFrame,
                    isDelta: Boolean,
                    tableName: String,
                    idFields: List[String],
                    idType: String,
                    source: String,
                    processType: String,
                    processId: String,
                    userId: String,
                    validStartTimeField: Option[(String, String)] = None,
                    validEndTimeField: Option[(String, String)] = None,
                    deleteIndicatorField: Option[(String, Any)] = None,
                    partitionKeys: Option[List[String]] = None,
                    newNames: Map[String, String] = Map(),
                    overwrite: Boolean = false,
                    writeChangeTables: Boolean = false)

  def loadLink(df: DataFrame,
               isDelta: Boolean,
               entityType1: String, idFields1: List[String], idType1: String,
               entityType2: String, idFields2: List[String], idType2: String,
               source: String,
               processType: String,
               processId: String,
               userId: String,
               tableName: Option[String] = None,
               validStartTimeField: Option[(String, String)] = None,
               validEndTimeField: Option[(String, String)] = None,
               deleteIndicatorField: Option[(String, Any)] = None,
               overwrite: Boolean = false)

  def loadHub(df: DataFrame, entityType: String, idFields: List[String], idType: String, processId: String)

  def registerCustomers(df: DataFrame, idFields: List[String], idType: String, processId: String)

}

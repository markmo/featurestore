package diamond.load

import diamond.AppConfig
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}

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

  val conf: AppConfig

  import conf.data._

  val META_ENTITY_ID = meta.entityId
  val META_START_TIME = meta.startTime
  val META_END_TIME = meta.endTime
  val META_SOURCE = meta.source
  val META_PROCESS_TYPE = meta.processType
  val META_PROCESS_ID = meta.processId
  val META_PROCESS_DATE = meta.processDate
  val META_USER_ID = meta.userId
  val META_HASHED_VALUE = meta.hashedValue
  val META_RECTYPE = meta.rectype
  val META_VERSION = meta.version
  val META_OPEN_END_DATE_VALUE = meta.openEndDateValue
  val META_VALID_START_TIME_FIELD = meta.validStartTimeField
  val META_VALID_END_TIME_FIELD = meta.validEndTimeField
  val META_VALID_START_TIME = meta.validStartTime
  val META_VALID_END_TIME = meta.validEndTime
  val META_DELETE_INDICATOR_FIELD = meta.deleteIndicatorField

  val RECTYPE_INSERT = rectype.insert
  val RECTYPE_UPDATE = rectype.update
  val RECTYPE_DELETE = rectype.delete

  val BASE_URI = baseURI

  val customerHubSchema = StructType(
    StructField("entity_id", StringType) ::
    StructField("customer_id", StringType) ::
    StructField("customer_id_type", StringType) ::
    StructField("process_time", StringType) :: Nil
  )

  val procSchema = StructType(
    StructField(META_PROCESS_ID, StringType) ::
    StructField(META_PROCESS_TYPE, StringType) ::
    StructField(META_USER_ID, StringType) ::
    StructField("read_count", LongType) ::
    StructField("duplicates_count", LongType) ::
    StructField("inserts_count", LongType) ::
    StructField("updates_count", LongType) ::
    StructField("deletes_count", LongType) ::
    StructField("process_time", TimestampType) ::
    StructField(META_PROCESS_DATE, DateType) :: Nil
  )

  def loadAll(sqlContext: SQLContext,
              processType: String,
              processId: String,
              userId: String)

  protected def loadAllInternal(sqlContext: SQLContext,
                                read: String => DataFrame,
                                processType: String,
                                processId: String,
                                userId: String) = {

    import conf.data.acquisition._

    for ((_, hub) <- hubs) {
      import hub._
      val df = read(source)
      loadHub(df, isDelta, entityType, idFields, idType, source,
        processType, processId, userId, tableName,
        validStartTimeField, validEndTimeField, deleteIndicatorField,
        newNames, overwrite)
    }

    for ((_, sat) <- satellites) {
      import sat._
      val df = read(source)
      loadSatellite(df, isDelta, tableName, idFields, idType, source,
        processType, processId, userId, projection,
        validStartTimeField, validEndTimeField, deleteIndicatorField,
        partitionKeys, newNames, overwrite, writeChangeTables)
    }

    for ((_, lnk) <- links) {
      import lnk._
      val df = read(source)
      loadLink(df, isDelta,
        entityType1, idFields1, idType1,
        entityType2, idFields2, idType2,
        source, processType, processId, userId, tableName,
        validStartTimeField, validEndTimeField, deleteIndicatorField,
        overwrite)
    }

    for ((_, map) <- mappings) {
      import map._
      val df = read(source)
      loadMapping(df, isDelta, entityType,
        idFields1, idType1,
        idFields2, idType2,
        confidence, source,
        processType, processId, userId, tableName,
        validStartTimeField, validEndTimeField, deleteIndicatorField,
        overwrite)
    }
  }

  def registerCustomers(df: DataFrame,
                        isDelta: Boolean,
                        idField: String, idType: String,
                        source: String,
                        processType: String,
                        processId: String,
                        userId: String): Unit

  def registerServices(df: DataFrame,
                       isDelta: Boolean,
                       idField: String, idType: String,
                       source: String,
                       processType: String,
                       processId: String,
                       userId: String): Unit

  def loadHub(df: DataFrame,
              isDelta: Boolean,
              entityType: String, idFields: List[String], idType: String,
              source: String,
              processType: String,
              processId: String,
              userId: String,
              tableName: Option[String] = None,
              validStartTimeField: Option[(String, String)] = None,
              validEndTimeField: Option[(String, String)] = None,
              deleteIndicatorField: Option[(String, Any)] = None,
              newNames: Map[String, String] = Map(),
              overwrite: Boolean = false): Unit

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
    * @param projection Option[List[String]] optional list of fields to load from the source
    *                   table
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
                    projection: Option[List[String]] = None,
                    validStartTimeField: Option[(String, String)] = None,
                    validEndTimeField: Option[(String, String)] = None,
                    deleteIndicatorField: Option[(String, Any)] = None,
                    partitionKeys: Option[List[String]] = None,
                    newNames: Map[String, String] = Map(),
                    overwrite: Boolean = false,
                    writeChangeTables: Boolean = false): Unit

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
               overwrite: Boolean = false): Unit

  def loadMapping(df: DataFrame,
                  isDelta: Boolean,
                  entityType: String,
                  idFields1: List[String], idType1: String,
                  idFields2: List[String], idType2: String,
                  confidence: Double,
                  source: String,
                  processType: String,
                  processId: String,
                  userId: String,
                  tableName: Option[String] = None,
                  validStartTimeField: Option[(String, String)] = None,
                  validEndTimeField: Option[(String, String)] = None,
                  deleteIndicatorField: Option[(String, Any)] = None,
                  overwrite: Boolean = false): Unit

  def readCurrentMapping(sqlContext: SQLContext, entityType: String, tableName: Option[String] = None): DataFrame

  def readMapping(sqlContext: SQLContext, entityType: String, tableName: Option[String] = None): DataFrame

}

package diamond.load

import com.github.nscala_time.time.Imports._
import diamond.AppConfig
import diamond.utility.functions._
import diamond.utility.udfs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.json4s.native.Serialization

/**
  * Depends on Hive Update: Hive version 0.14+
  * and transaction support configured on server.
  *
  * The following steps are required to setup transaction support.
  *
  * Set the following properties in conf/hive-site.xml
  *
  * {{{
  * <property>
  *   <name>hive.support.concurrency</name>
  *   <value>true</value>
  * </property>
  * <property>
  *   <name>hive.enforce.bucketing</name>
  *   <value>true</value>
  * </property>
  * <property>
  *   <name>hive.exec.dynamic.partition.mode</name>
  *   <value>nonstrict</value>
  * </property>
  * <property>
  *   <name>hive.txn.manager</name>
  *   <value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
  * </property>
  * <property>
  *   <name>hive.compactor.initiator.on</name>
  *   <value>true</value>
  * </property>
  * <property>
  *   <name>hive.compactor.worker.threads</name>
  *   <value>2</value>
  * </property>
  * }}}
  *
  * In addition, as this feature is still in development, the following
  * configuration must be added in conf/hive-site.xml.
  *
  * {{{
  * <property>
  *   <name>hive.in.test</name>
  *   <value>true</value>
  * </property>
  * }}}
  *
  * Only ORC file format is supported in this first release. The feature has
  * been built such that transactions can be used by any storage format that
  * can determine how updates or deletes apply to base records (basically,
  * that has an explicit or implicit row id), but so far the integration work
  * has only been done for ORC.
  *
  * Also, tables must be bucketed to make use of these features. For example,
  *
  * {{{
  * create table if not exists customer_hub(
  *   entity_id STRING
  *   ,entity_type STRING
  *   ,customer_id string
  *   ,...
  *   ,version INT)
  *    clustered by (entity_id) into 2 buckets
  *    stored as orc TBLPROPERTIES('transactional'='true')
  * }}}
  *
  * However, even with transaction support enabled, there isn't support in
  * Spark SQL yet for update or delete in Hive.
  *
  * Tips:
  * Use SQLContext.sql to periodically run ALTER TABLE…​CONCATENATE to merge
  * many small files into larger files optimized for the HDFS block size.
  * Since the CONCATENATE command operates on files in place it is transparent
  * to any downstream processing.
  *
  * TODO
  * * Add 'factory' schema for tables
  *
  * Created by markmo on 23/01/2016.
  */
class HiveDataLoader(implicit val conf: AppConfig) extends DataLoader {

  def loadAll(sqlContext: SQLContext,
              processType: String,
              processId: String,
              userId: String) =
    loadAllInternal(sqlContext,
      source => sqlContext.sql(s"select * from $source"),
      processType, processId, userId)

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
              overwrite: Boolean = false // irrelevant
             ): Unit = {

    val tn = if (tableName.isDefined) tableName.get else s"${entityType.toLowerCase}_hub"
    val pk = idFields.map(f => newNames.getOrElse(f, f))
    val colTypeMap = df.schema.fields.map(f => (newNames.getOrElse(f.name, f.name), f.dataType match {
      case IntegerType => "INT"
      case t => t.typeName
    })).toMap
    val idCols = pk.map(f => s"$f ${colTypeMap(f)}").mkString(",")
    val sqlContext = df.sqlContext
    sqlContext.sql(
      s"""
         |create table if not exists $tn(
         |$META_ENTITY_ID STRING
         |,$META_ENTITY_TYPE STRING
         |,$idCols
         |,$META_ID_TYPE STRING
         |,$META_START_TIME TIMESTAMP
         |,$META_END_TIME TIMESTAMP
         |,$META_SOURCE STRING
         |,$META_PROCESS_TYPE STRING
         |,$META_PROCESS_ID STRING
         |,$META_PROCESS_DATE DATE
         |,$META_USER_ID STRING
         |,$META_VALID_START_TIME TIMESTAMP
         |,$META_VALID_END_TIME TIMESTAMP
         |,$META_RECTYPE STRING
         |,$META_VERSION INT)
         | clustered by ($META_ENTITY_ID) into 2 buckets
         | stored as orc TBLPROPERTIES('transactional'='true')
       """.stripMargin)

    sqlContext.udf.register("hashKey", hashKey(_: String))
    val renamed = newNames.foldLeft(df)({
      case (d, (oldName, newName)) => d.withColumnRenamed(oldName, newName)
    })

    // dedup
    val in = renamed.distinct()
    in.registerTempTable("imported")
    val inIdCols = pk.map("i." + _).mkString(",")
    val (validStartTimeExpr, validEndTimeExpr) =
      if (validStartTimeField.isDefined && validEndTimeField.isDefined) {
        (
          s"convertStringToTimestamp(i.${validStartTimeField.get._1}, '${validStartTimeField.get._2}'",
          s"convertStringToTimestamp(i.${validEndTimeField.get._1}, '${validEndTimeField.get._2}'"
          )
      } else {
        ("current_timestamp()", s"'$META_OPEN_END_DATE_VALUE'")
      }

    val joinPredicates = pk.map(f => s"e.$f = i.$f").mkString(" and ")
    val sqlNewEntities =
      s"""
         |insert into $tn
         |select hashKey(concat('$idType',$inIdCols)) as $META_ENTITY_ID
         |,'$entityType' as $META_ENTITY_TYPE
         |,$inIdCols
         |,'$idType' as $META_ID_TYPE
         |,current_timestamp() as $META_START_TIME
         |,'$META_OPEN_END_DATE_VALUE' as $META_END_TIME
         |,'$source' as $META_SOURCE
         |,'$processType' as $META_PROCESS_TYPE
         |,'$processId' as $META_PROCESS_ID
         |,current_date() as $META_PROCESS_DATE
         |,'$userId' as $META_USER_ID
         |,$validStartTimeExpr as $META_VALID_START_TIME
         |,$validEndTimeExpr as $META_VALID_END_TIME
         |,'$RECTYPE_INSERT' as $META_RECTYPE
         |,1 as $META_VERSION
         |from imported i
         |left join $tn e on $joinPredicates and e.$META_ID_TYPE = '$idType'
         |where e.$META_ENTITY_ID is null
       """.stripMargin

    val (insertsCount: Long, deletesCount: Long) = if (deleteIndicatorField.isDefined) {
      val delIndField = deleteIndicatorField.get._1
      val delIndFieldVal = deleteIndicatorField.get._2.toString
      val delIndFieldLit = if (isNumber(delIndFieldVal)) delIndFieldVal else s"'$delIndFieldVal'"

      (
        sqlContext.sql(
          s"""
             |$sqlNewEntities
             |and i.$delIndField <> $delIndFieldLit
           """.stripMargin).count(),

        0L

        // update query with join not supported in hive
//        sqlContext.sql(
//          s"""
//             |update $tn
//             |set $META_END_TIME = current_timestamp()
//             |,$META_RECTYPE = '$RECTYPE_DELETE'
//             |from $tn e
//             |join imported i on $joinPredicates
//             |where e.$META_ID_TYPE = '$idType'
//             |and i.$delIndField = $delIndFieldLit
//           """.stripMargin).count()

        // the following is supported in hive 0.14 with transaction support enabled
        // but not supported in hive on spark yet
//        sqlContext.sql(
//          s"""
//             |update $tn
//             |set $META_END_TIME = current_timestamp()
//             |,$META_RECTYPE = '$RECTYPE_DELETE'
//             |where $META_ID_TYPE = '$idType'
//             |and concat(${pk.mkString(",")}) in (
//             |select concat(${pk.map("i." + _).mkString(",")}) from imported i
//             |where i.$META_ID_TYPE = '$idType'
//             |and i.$delIndField = $delIndFieldLit
//             |)
//           """.stripMargin).count()
        )
    } else if (!isDelta) {
      (
        sqlContext.sql(sqlNewEntities).count(),
        0L
//        sqlContext.sql(
//          s"""
//             |update $tn
//             |set $META_END_TIME = current_timestamp()
//             |,$META_RECTYPE = '$RECTYPE_DELETE'
//             |from $tn e
//             |left join imported i on $joinPredicates
//             |where e.$META_ID_TYPE = '$idType'
//             |and i.${pk.head} is null
//           """.stripMargin).count()

        // the following is supported in hive 0.14 with transaction support enabled
        // but not supported in hive on spark yet
//        sqlContext.sql(
//          s"""
//             |update $tn
//             |set $META_END_TIME = current_timestamp()
//             |,$META_RECTYPE = '$RECTYPE_DELETE'
//             |where $META_ID_TYPE = '$idType'
//             |and concat(${pk.mkString(",")}) not in (
//             |select concat(${pk.map("i." + _).mkString(",")}) from imported i
//             |where i.$META_ID_TYPE = '$idType'
//             |)
//           """.stripMargin).count()
        )

    } else {
      (sqlContext.sql(sqlNewEntities).count(), 0L)
    }
    val now = DateTime.now
    val readCount = df.count()
    writeProcessLog(sqlContext, processId, processType, userId,
      readCount, readCount - in.count(), insertsCount, 0,
      deletesCount, now, now)

    val metadata = Map(
      "entityType" -> entityType,
      "idFields" -> idFields,
      "idType" -> idType,
      "newNames" -> newNames,
      "validStartTimeField" -> validStartTimeField,
      "validEndTimeField" -> validEndTimeField,
      "deleteIndicatorField" -> deleteIndicatorField
    )
    writeMetaLog(sqlContext, tn, metadata, userId)
  }

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
                    overwrite: Boolean = true, // irrelevant
                    writeChangeTables: Boolean = false // irrelevant
                   ): Unit = {

    // dedup
    val distinct = if (projection.isDefined) {
      df.select(projection.get.map(col): _*).distinct()
    } else {
      df.distinct()
    }
    val renamed = newNames.foldLeft(distinct)({
      case (d, (oldName, newName)) => d.withColumnRenamed(oldName, newName)
    })
    val pk = idFields.map(f => newNames.getOrElse(f, f))
    val baseNames = renamed.schema.fieldNames.toList diff pk
    val t = renamed
      .withColumn(META_ENTITY_ID, hashKeyUDF(concat(lit(idType), concat(idFields.map(col): _*))))
      .withColumn(META_START_TIME, current_timestamp().cast(TimestampType))
      .withColumn(META_END_TIME, lit(META_OPEN_END_DATE_VALUE).cast(TimestampType))
      .withColumn(META_PROCESS_ID, lit(processId))
      .withColumn(META_PROCESS_DATE, current_date())
      .withColumn(META_HASHED_VALUE, fastHashUDF(concat(baseNames.map(col): _*)))

    val in = if (validStartTimeField.isDefined && validEndTimeField.isDefined) {
      t
        .withColumn(META_VALID_START_TIME, convertStringToTimestampUDF(col(validStartTimeField.get._1), lit(validStartTimeField.get._2)))
        .withColumn(META_VALID_END_TIME, convertStringToTimestampUDF(col(validEndTimeField.get._1), lit(validEndTimeField.get._2)))
    } else {
      t
    }

    // add column headers for process metadata
    val names = META_ENTITY_ID :: baseNames ++
      List(
        META_VALID_START_TIME, META_VALID_END_TIME,
        META_START_TIME, META_END_TIME,
        META_PROCESS_DATE, META_HASHED_VALUE)

    val header = names ++ List(META_RECTYPE, META_VERSION)

    val sqlContext = df.sqlContext
    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
    val now = DateTime.now()
    val tableExist = try {
      sqlContext.sql(s"select count(*) from $tableName").head().getInt(0) > 0
    } catch {
      case _: Throwable => false
    }
    if (tableExist) {
      // current records are where `end_time = '9999-12-31'`
      val ex = sqlContext.sql(
        s"""
           |select * from $tableName
           |where $META_END_TIME = '$META_OPEN_END_DATE_VALUE'
           |and $META_RECTYPE <> '$RECTYPE_DELETE'
         """.stripMargin).cache()

      // records in the new set that aren't in the existing set
      val newRecords = in
        .join(ex, in(META_ENTITY_ID) === ex(META_ENTITY_ID), "left_outer")
        .where(ex(META_ENTITY_ID).isNull)
        .select(names.map(in(_)): _*)
        .withColumn(META_RECTYPE, lit(RECTYPE_INSERT))
        .withColumn(META_VERSION, lit(1))

      // records in the new set that are also in the existing set
      // but whose values have changed
      val changed = in
        .join(ex, META_ENTITY_ID)
        .where(in(META_HASHED_VALUE) !== ex(META_HASHED_VALUE))
        .withColumn(META_RECTYPE, lit(RECTYPE_UPDATE))
        .withColumn(META_VERSION, lit(ex(META_VERSION) + 1))
        .select(names.map(in(_)) ++ List(col(META_RECTYPE), col(META_VERSION)): _*)

      val (inserts, updates, deletes) =
        if (deleteIndicatorField.isDefined) {
          val deletesNew = newRecords
            .where(col(deleteIndicatorField.get._1) === lit(deleteIndicatorField.get._2))
            .withColumn(META_RECTYPE, lit(RECTYPE_DELETE))

          val deletesExisting = changed
            .where(col(deleteIndicatorField.get._1) === lit(deleteIndicatorField.get._2))
            .select(in(META_START_TIME) :: header.map(ex(_)): _*)
            .withColumn(META_END_TIME, lit(in(META_START_TIME)))
            .drop(in(META_START_TIME))
            .withColumn(META_RECTYPE, lit(RECTYPE_DELETE))

          (
            // inserts
            newRecords.where(col(deleteIndicatorField.get._1) !== lit(deleteIndicatorField.get._2)),

            // changes
            changed.where(col(deleteIndicatorField.get._1) !== lit(deleteIndicatorField.get._2)),

            // deletes
            if (overwrite) {
              Some(deletesExisting
                .withColumn(META_VERSION, ex(META_VERSION) + lit(1))
                .unionAll(deletesNew))
            } else {
              Some(deletesExisting.unionAll(deletesNew))
            }
            )
        } else if (!isDelta) {
          val deletesExisting = ex
            .join(in, ex(META_ENTITY_ID) === in(META_ENTITY_ID), "left_outer")
            .where(in(META_ENTITY_ID).isNull)
            .select(header.map(ex(_)): _*)
            .withColumn(META_END_TIME, current_timestamp().cast(TimestampType))
            .withColumn(META_RECTYPE, lit(RECTYPE_DELETE))

          (
            // inserts
            newRecords,

            // changes
            changed,

            // deletes
            if (overwrite) {
              Some(deletesExisting)
            } else {
              Some(deletesExisting.withColumn(META_VERSION, ex(META_VERSION) + lit(1)))
            }
            )
        } else {
          (newRecords, changed, None)
        }

      updates.cache().registerTempTable("updated")

      // update query with join not supported in hive
//      sqlContext.sql(
//        s"""
//           |update $tableName
//           |set $META_END_TIME = u.$META_START_TIME
//           |,$META_RECTYPE = '$RECTYPE_UPDATE'
//           |from $tableName e
//           |inner join updated u on u.$META_ENTITY_ID = e.$META_ENTITY_ID
//           |where e.$META_END_TIME = '$META_OPEN_END_DATE_VALUE'
//         """.stripMargin)

      // the following is supported in hive 0.14 with transaction support enabled
      // but not supported in hive on spark yet
//      sqlContext.sql(
//        s"""
//           |update $tableName
//           |set $META_END_TIME = u.$META_START_TIME
//           |,$META_RECTYPE = '$RECTYPE_UPDATE'
//           |where $META_END_TIME = '$META_OPEN_END_DATE_VALUE'
//           |and $META_ENTITY_ID in (
//           |select u.$META_ENTITY_ID from updated u
//           |)
//         """.stripMargin)

      if (deletes.isDefined) {
        deletes.get.cache().registerTempTable("deleted")

//        sqlContext.sql(
//          s"""
//             |update $tableName
//             |set $META_END_TIME = u.$META_START_TIME
//             |,$META_RECTYPE = '$RECTYPE_DELETE'
//             |from $tableName e
//             |inner join deleted u on u.$META_ENTITY_ID = e.$META_ENTITY_ID
//             |where e.$META_END_TIME = '$META_OPEN_END_DATE_VALUE'
//           """.stripMargin)

        // the following is supported in hive 0.14 with transaction support enabled
        // but not supported in hive on spark yet
//        sqlContext.sql(
//          s"""
//             |update $tableName
//             |set $META_END_TIME = u.$META_START_TIME
//             |,$META_RECTYPE = '$RECTYPE_DELETE'
//             |where $META_END_TIME = '$META_OPEN_END_DATE_VALUE'
//             |and $META_ENTITY_ID in (
//             |select d.$META_ENTITY_ID from deleted d
//             |)
//           """.stripMargin)
      }

      val all = deletes match {
        case Some(d) => inserts.unionAll(updates).unionAll(d)
        case None => inserts.unionAll(updates)
      }

      val writer =
        if (partitionKeys.isDefined) {
          all.write.mode(saveMode).partitionBy(partitionKeys.get: _*)
        } else {
          all.write.mode(saveMode)
        }

      writer.saveAsTable(tableName)

      if (deletes.isDefined) deletes.get.unpersist()
      updates.unpersist()

    } else {
      val out = idFields.foldLeft(in)({
        case (d, idField) => d.drop(idField)
      })
      val w = out
        .withColumn(META_RECTYPE, lit(RECTYPE_INSERT))
        .withColumn(META_VERSION, lit(1))
        .write
        .mode(SaveMode.Append)

      val writer = if (partitionKeys.isDefined) w.partitionBy(partitionKeys.get: _*) else w

      writer.saveAsTable(tableName)
    }
  }

  def loadLink(df: DataFrame,
               isDelta: Boolean,
               srcEntityType: String, srcIdFields: List[String], srcIdType: String,
               dstEntityType: String, dstIdFields: List[String], dstIdType: String,
               source: String,
               processType: String,
               processId: String,
               userId: String,
               tableName: Option[String] = None,
               validStartTimeField: Option[(String, String)] = None,
               validEndTimeField: Option[(String, String)] = None,
               deleteIndicatorField: Option[(String, Any)] = None,
               overwrite: Boolean = false): Unit = ???

  def loadMapping(df: DataFrame,
                  isDelta: Boolean,
                  entityType: String,
                  srcIdFields: List[String], srcIdType: String,
                  dstIdFields: List[String], dstIdType: String,
                  confidence: Double,
                  source: String,
                  processType: String,
                  processId: String,
                  userId: String,
                  tableName: Option[String] = None,
                  validStartTimeField: Option[(String, String)] = None,
                  validEndTimeField: Option[(String, String)] = None,
                  deleteIndicatorField: Option[(String, Any)] = None,
                  overwrite: Boolean = false): Unit = ???

  def readCurrentMapping(sqlContext: SQLContext, entityType: String, tableName: Option[String] = None) = ???

  def readMapping(sqlContext: SQLContext, entityType: String, tableName: Option[String] = None) = ???

  def writeProcessLog(sqlContext: SQLContext,
                      processId: String,
                      processType: String,
                      userId: String,
                      readCount: Long,
                      duplicatesCount: Long,
                      insertsCount: Long,
                      updatesCount: Long,
                      deletesCount: Long,
                      processTime: DateTime,
                      processDate: DateTime): Unit = {

    sqlContext.sql("create table if not exists dual (dummy STRING)")
    sqlContext.sql(
      s"""
         |create table if not exists process_log(
         |$META_PROCESS_ID STRING
         |,$META_PROCESS_TYPE STRING
         |,$META_USER_ID STRING
         |,read_count BIGINT
         |,duplicates_count BIGINT
         |,inserts_count BIGINT
         |,updates_count BIGINT
         |,deletes_count BIGINT
         |,process_time TIMESTAMP
         |)
       """.stripMargin)

    sqlContext.sql(
      s"""
         |insert into process_log
         |select
         |'$processId'
         |,'$processType'
         |,'$userId'
         |,$readCount
         |,$duplicatesCount
         |,$insertsCount
         |,$updatesCount
         |,$deletesCount
         |,from_unixtime(${processTime.getMillis})
         |from dual
       """.stripMargin)
  }

  def writeMetaLog(sqlContext: SQLContext,
                   tableName: String,
                   metadata: Map[String, Any],
                   userId: String): Unit = {

    implicit val formats = org.json4s.DefaultFormats
    val metaJson = Serialization.write(metadata)

    // TODO
    // do on application startup
    sqlContext.sql("create table if not exists dual (dummy STRING)")

    sqlContext.sql(
      s"""
         |create table if not exists meta_log(
         |table_name STRING
         |,meta_json STRING
         |,created TIMESTAMP
         |,$META_USER_ID STRING)
       """.stripMargin)

    sqlContext.sql(
      s"""
         |insert into meta_log
         |select '$tableName', '$metaJson', current_timestamp(), '$userId'
         |from dual
       """.stripMargin)
  }

}

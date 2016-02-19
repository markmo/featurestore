package diamond.load

import com.github.nscala_time.time.Imports._
import diamond.AppConfig
import diamond.utility.functions._
import diamond.utility.udfs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Depends on Hive Update: Hive version 0.14+
  * and transaction support configured on server.
  *
  * Created by markmo on 23/01/2016.
  */
class HiveDataLoader(implicit val conf: AppConfig) extends DataLoader {

  def loadAll(sqlContext: SQLContext,
              processType: String,
              processId: String,
              userId: String) = ???

  def registerCustomers(df: DataFrame,
                        isDelta: Boolean,
                        idField: String, idType: String,
                        source: String,
                        processType: String,
                        processId: String,
                        userId: String): Unit = ???

  def registerServices(df: DataFrame,
                       isDelta: Boolean,
                       idField: String, idType: String,
                       source: String,
                       processType: String,
                       processId: String,
                       userId: String): Unit = ???

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
              overwrite: Boolean = false): Unit = {

    val sqlContext = df.sqlContext
    sqlContext.sql(
      """
        |create table if not exists customer_hub(
        |entity_id STRING
        |,customer_id STRING
        |,id_type STRING
        |,process_time TIMESTAMP)
      """.stripMargin)
    sqlContext.udf.register("hashKey", hashKey(_: String))
    df.registerTempTable("imported")
    sqlContext.sql(
      s"""
         |insert into customer_hub
         |select hashKey(concat('$idType',i.${idFields(0)})) as entity_id
         |,i.${idFields(0)} as customer_id
         |,'$idType' as id_type
         |,current_timestamp() as process_time
         |from imported i
         |left join customer_hub e on e.customer_id = i.${idFields(0)} and e.id_type = '$idType'
         |where e.entity_id is null
      """.stripMargin)
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
           |and rectype <> 'D'
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

      sqlContext.sql(
        s"""
           |update $tableName
           |set $META_END_TIME = u.$META_START_TIME
           |,$META_RECTYPE = '$RECTYPE_UPDATE'
           |from $tableName e
           |inner join updated u on u.$META_ENTITY_ID = e.$META_ENTITY_ID
           |where e.$META_END_TIME = '$META_OPEN_END_DATE_VALUE'
        """.stripMargin)

      if (deletes.isDefined) {
        deletes.get.cache().registerTempTable("deleted")

        sqlContext.sql(
          s"""
             |update $tableName
             |set $META_END_TIME = u.$META_START_TIME
             |,$META_RECTYPE = '$RECTYPE_DELETE'
             |from $tableName e
             |inner join deleted u on u.$META_ENTITY_ID = e.$META_ENTITY_ID
             |where e.$META_END_TIME = '$META_OPEN_END_DATE_VALUE'
        """.stripMargin)
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
               overwrite: Boolean = false): Unit = ???

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
                  overwrite: Boolean = false): Unit = ???

  def readCurrentMapping(sqlContext: SQLContext, entityType: String, tableName: Option[String] = None) = ???

  def readMapping(sqlContext: SQLContext, entityType: String, tableName: Option[String] = None) = ???

}

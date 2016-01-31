package diamond.load

import diamond.utility.functions._
import diamond.utility.udfs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Depends on Hive Update: Hive version 0.14+
  * and transaction support configured on server.
  *
  * Created by markmo on 23/01/2016.
  */
class HiveDataLoader extends DataLoader {

  def loadSatellite(df: DataFrame,
                    isDelta: Boolean,
                    tableName: String,
                    idField: String,
                    idType: String,
                    deleteIndicatorField: Option[(String, Any)] = None,
                    partitionKeys: Option[List[String]] = None,
                    newNames: Map[String, String] = Map(),
                    overwrite: Boolean = true, // irrelevant
                    writeChangeTables: Boolean = false // irrelevant
                   ) {
    val renamed = newNames.foldLeft(df)({
      case (d, (oldName, newName)) => d.withColumnRenamed(oldName, newName)
    })
    val baseNames = renamed.schema.fieldNames.toList diff List(idField)
    val in = renamed
      .withColumn(META_ENTITY_ID, hashKeyUDF(concat(lit(idType), col(idField))))
      .withColumn(META_START_TIME, current_timestamp().cast(TimestampType))
      .withColumn(META_END_TIME, lit(META_OPEN_END_DATE_VALUE).cast(TimestampType))
      .withColumn(META_PROCESS_DATE, current_date())
      .withColumn(META_HASHED_VALUE, fastHashUDF(concat(baseNames.map(col): _*)))

    // add column headers for process metadata
    val names = META_ENTITY_ID :: baseNames ++ List(META_START_TIME, META_END_TIME, META_PROCESS_DATE, META_HASHED_VALUE)
    val header = names ++ List(META_RECTYPE, META_VERSION)

    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
    val sqlContext = df.sqlContext
    val tableExist = try {
      sqlContext.sql(s"select count(*) from $tableName").head().getInt(0) > 0
    } catch {
      case _: Throwable => false
    }
    if (tableExist) {
      // current records are where `end_time = '9999-12-31'`
      val ex = sqlContext.sql(s"select * from $tableName where $META_END_TIME = '$META_OPEN_END_DATE_VALUE'")

      // records in the new set that aren't in the existing set
      val newRecords = in
        .join(ex, in(META_ENTITY_ID) === ex(META_ENTITY_ID), "left")
        .where(ex(META_ENTITY_ID).isNull)
        .select(names.map(in(_)): _*)
        .withColumn(META_RECTYPE, lit(RECTYPE_INSERT))
        .withColumn(META_VERSION, lit(1))

      // records in the new set that are also in the existing set
      val matchedRecords = in
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

          val deletesExisting = matchedRecords
            .where(col(deleteIndicatorField.get._1) === lit(deleteIndicatorField.get._2))
            .withColumn(META_RECTYPE, lit(RECTYPE_DELETE))
            .drop(in(META_END_TIME))
            .withColumn(META_END_TIME, lit(in(META_START_TIME)))
            .select(header.map(ex(_)): _*)

          (
            // inserts
            newRecords.where(col(deleteIndicatorField.get._1) !== lit(deleteIndicatorField.get._2)),
            // changes
            matchedRecords.where(col(deleteIndicatorField.get._1) !== lit(deleteIndicatorField.get._2)),
            // deletes
            Some(deletesNew.unionAll(deletesExisting))
            )
        } else if (!isDelta) {
          (
            // inserts
            newRecords,
            // changes
            matchedRecords,
            // deletes
            Some(ex
              .join(in, ex(META_ENTITY_ID) === in(META_ENTITY_ID), "left")
              .where(in(META_ENTITY_ID).isNull)
              .withColumn(META_RECTYPE, lit(RECTYPE_DELETE))
              .withColumn(META_VERSION, lit(ex(META_VERSION) + 1))
              .select(header.map(ex(_)): _*)
            )
            )
        } else {
          (newRecords, matchedRecords, None)
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
      val w = in
        .drop(idField)
        .withColumn(META_RECTYPE, lit(RECTYPE_INSERT))
        .withColumn(META_VERSION, lit(1))
        .write
        .mode(SaveMode.Append)

      val writer = if (partitionKeys.isDefined) w.partitionBy(partitionKeys.get: _*) else w

      writer.saveAsTable(tableName)
    }
  }

  def registerCustomers(df: DataFrame, idField: String, idType: String) {
    val sqlContext = df.sqlContext
    sqlContext.sql(
      """
        |create table if not exists customer_hub(
        |entity_id STRING
        |,customer_id STRING
        |,customer_id_type STRING
        |,process_time TIMESTAMP)
      """.stripMargin)
    sqlContext.udf.register("hashKey", hashKey(_: String))
    df.registerTempTable("imported")
    sqlContext.sql(
      s"""
         |insert into customer_hub
         |select hashKey(concat('$idType',i.$idField)) as entity_id
         |,i.$idField as customer_id
         |,'$idType' as customer_id_type
         |,current_timestamp() as process_time
         |from imported i
         |left join customer_hub e on e.customer_id = i.$idField and e.customer_id_type = '$idType'
         |where e.entity_id is null
      """.stripMargin)
  }

}

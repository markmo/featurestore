package diamond.load

import diamond.utility.functions._
import diamond.utility.udfs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Created by markmo on 23/01/2016.
  */
class HiveDataLoader extends DataLoader {

  def loadSatellite(df: DataFrame,
                    isDelta: Boolean,
                    tableName: String,
                    idField: String,
                    idType: String,
                    partitionKeys: Option[List[String]] = None,
                    newNames: Map[String, String] = Map()) {
    val names = df.schema.fieldNames.toList
    val hashed = df
      .withColumn(META_ENTITY_ID, hashKeyUDF(concat(lit(idType), col(idField))))
      .withColumn(META_START_TIME, current_timestamp().cast(TimestampType))
      .withColumn(META_END_TIME, lit(META_OPEN_END_DATE_VALUE).cast(TimestampType))
      .withColumn(META_PROCESS_DATE, current_date())
    val renamed = newNames.foldLeft(hashed)({
      case (d, (oldName, newName)) => d.withColumnRenamed(oldName, newName)
    })
    val updatedNames = newNames.foldLeft(names)({
      case (l: List[String], (oldName: String, newName: String)) => l.updated(l.indexOf(oldName), newName)
    }) diff List(idField)

    // add column headers for process metadata
    val satNames: List[String] = updatedNames ++ List(META_START_TIME, META_END_TIME, META_PROCESS_DATE)
    val sat = renamed.select(META_ENTITY_ID, satNames: _*)

    val sqlContext = df.sqlContext
    val tableExist = try {
      sqlContext.sql(s"select count(*) from $tableName").head().getInt(0) > 0
    } catch {
      case _: Throwable => false
    }
    if (tableExist) {
      val existing = sqlContext.sql(s"from $tableName select *")

      val headers: List[String] = META_ENTITY_ID :: satNames
      val added = sat
        .join(existing, sat(META_ENTITY_ID) === existing(META_ENTITY_ID), "left")
        .where(existing(META_ENTITY_ID).isNull)
        .select(headers.map(sat(_)): _*)

      val he = hashRows(existing, updatedNames)
      val hi = hashRows(sat, updatedNames)
      val updated = hi
        .join(he, META_ENTITY_ID)
        .where(hi(META_HASHED_VALUE) !== he(META_HASHED_VALUE))
        .select(headers.map(sat(_)): _*)
      updated.registerTempTable("updated")

      sqlContext.sql(
        s"""
           |update $tableName
           |set $META_END_TIME = u.$META_START_TIME
           |from $tableName e
           |inner join updated u on u.$META_ENTITY_ID = e.$META_ENTITY_ID
        """.stripMargin)

      updated.unionAll(added).write
        .partitionBy("process_date")
        .mode(SaveMode.Append)
        .saveAsTable(tableName)
    } else {
      sat.write
        .partitionBy("process_date")
        .mode(SaveMode.Append)
        .saveAsTable(tableName)
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

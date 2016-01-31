package diamond.load

import java.net.URI

import com.github.nscala_time.time.Imports._
import diamond.utility.functions._
import diamond.utility.udfs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/**
  * Parquet writes columns out of order (compared to the schema)
  * https://issues.apache.org/jira/browse/PARQUET-188
  * Fixed in 1.6.0
  *
  * Created by markmo on 23/01/2016.
  */
class ParquetDataLoader extends DataLoader {

  val FILE_EXT = ".parquet"
  val FILE_NEW = s"new$FILE_EXT"
  val FILE_CHANGED = s"changed$FILE_EXT"
  val FILE_REMOVED = s"removed$FILE_EXT"
  val FILE_CURRENT = s"current$FILE_EXT"

  val LAYER_IL = "il"

  val CUSTOMER_HUB_PATH = s"/$LAYER_IL/customer_hub$FILE_EXT"

  // TODO
  // Cover each scenario:
  // * Delta
  // * Full
  // * With effective dates
  // * Without
  // * With delete indicator
  // * Without
  // * Overwrite
  // * Append-only

  def loadSatellite(df: DataFrame,
                    isDelta: Boolean,
                    tableName: String,
                    idField: String,
                    idType: String,
                    deleteIndicatorField: Option[(String, Any)] = None,
                    partitionKeys: Option[List[String]] = None,
                    newNames: Map[String, String] = Map(),
                    overwrite: Boolean = false,
                    writeChangeTables: Boolean = false) {

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

    val tablePath = s"/$LAYER_IL/$tableName/$tableName$FILE_EXT"
    val currentPath = s"$BASE_URI/$LAYER_IL/$tableName/$FILE_CURRENT"
    val sqlContext = df.sqlContext
    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())

    if (fs.exists(new Path(tablePath))) {

      // need to use the __current__ set or join below will match multiple rows
      val ex = sqlContext.read.load(currentPath).cache()

      // with update capability, read would filter on `end_time = '9999-12-31'`
      // to select current records, but end_time is not being updated on old records

      // records in the new set that aren't in the existing set
      val newRecords = in
        .join(ex, in(META_ENTITY_ID) === ex(META_ENTITY_ID), "left")
        .where(ex(META_ENTITY_ID).isNull)
        .select(names.map(in(_)): _*)
        .withColumn(META_RECTYPE, lit(RECTYPE_INSERT))
        .withColumn(META_VERSION, lit(1))

      // records in the new set that are also in the existing set
      val matched = in
        .join(ex, META_ENTITY_ID)
        .where(in(META_HASHED_VALUE) !== ex(META_HASHED_VALUE))
        .cache()

      val (inserts, changes, deletes) =
        if (deleteIndicatorField.isDefined) {
          val deletesNew = newRecords
            .where(col(deleteIndicatorField.get._1) === lit(deleteIndicatorField.get._2))
            .withColumn(META_RECTYPE, lit(RECTYPE_DELETE))

          (
            // inserts
            newRecords.where(col(deleteIndicatorField.get._1) !== lit(deleteIndicatorField.get._2)),
            // changes
            matched.where(col(deleteIndicatorField.get._1) !== lit(deleteIndicatorField.get._2)),
            // deletes
            if (overwrite) {
              val deletesExisting = matched
                .where(col(deleteIndicatorField.get._1) === lit(deleteIndicatorField.get._2))
                .select(in(META_START_TIME) :: header.map(ex(_)): _*)
                .withColumn(META_END_TIME, in(META_START_TIME))
                .drop(in(META_START_TIME))
                .withColumn(META_RECTYPE, lit(RECTYPE_DELETE))

              Some(deletesNew.unionAll(deletesExisting))
            } else {
              Some(deletesNew)
            }
            )
        } else if (!isDelta) {
          (
            // inserts
            newRecords,
            // changes
            matched,
            // deletes
            Some(ex
              .join(in, ex(META_ENTITY_ID) === in(META_ENTITY_ID), "left")
              .where(in(META_ENTITY_ID).isNull)
              .withColumn(META_RECTYPE, lit(RECTYPE_DELETE))
              .withColumn(META_VERSION, ex(META_VERSION) + lit(1))
              .select(header.map(ex(_)): _*)
            )
            )
        } else {
          (newRecords, matched, None)
        }

      val updatesNew = changes
        .select(ex(META_VERSION).as("old_version") :: names.map(in(_)): _*)
        .withColumn(META_RECTYPE, lit(RECTYPE_UPDATE))
        .withColumn(META_VERSION, col("old_version") + lit(1))
        .drop("old_version")

      if (writeChangeTables) {
        inserts.cache()
        updatesNew.cache()
        if (deletes.isDefined) deletes.get.cache()
      }

      val updates =
        if (overwrite) {
          val updatesExisting = changes
            .select(in(META_START_TIME) :: names.map(ex(_)): _*)
            .withColumn(META_RECTYPE, lit(RECTYPE_UPDATE))
            .withColumn(META_END_TIME, in(META_START_TIME))
            .drop(in(META_START_TIME))

          updatesNew.unionAll(updatesExisting)
        } else {
          updatesNew
        }

      val all = deletes match {
        case Some(d) => inserts.unionAll(updates).unionAll(d)
        case None => inserts.unionAll(updates)
      }
      all.cache()

      val writer =
        if (partitionKeys.isDefined) {
          all.write.mode(saveMode).partitionBy(partitionKeys.get: _*)
        } else {
          all.write.mode(saveMode)
        }

      writer.parquet(s"$BASE_URI$tablePath")

      // write snapshot
      val latest: RDD[Row] = ex.unionAll(all)
        .map(row => (row.getAs[String](META_ENTITY_ID), row))
        .reduceByKey((a, b) => if (b.getAs[Int](META_VERSION) > a.getAs[Int](META_VERSION)) b else a)
        .map(_._2)

      val latestDF = sqlContext.createDataFrame(latest, ex.schema)

      latestDF
        .write
        .mode(SaveMode.Overwrite)
        .parquet(currentPath)

      if (writeChangeTables) {
        val daysAgo = 3
        writeChangeTable(fs, inserts, header, s"$BASE_URI/$LAYER_IL/$tableName/$FILE_NEW", daysAgo)
        writeChangeTable(fs, updatesNew, header, s"$BASE_URI/$LAYER_IL/$tableName/$FILE_CHANGED", daysAgo)

        updatesNew.unpersist()
        inserts.unpersist()

        if (deletes.isDefined) {
          writeChangeTable(fs, deletes.get, header, s"$BASE_URI/$LAYER_IL/$tableName/$FILE_REMOVED", daysAgo)
          deletes.get.unpersist()
        }
      }

      all.unpersist()
      matched.unpersist()
      ex.unpersist()

    } else {
      in.cache()
      val w = in
        .drop(idField)
        .withColumn(META_RECTYPE, lit(RECTYPE_INSERT))
        .withColumn(META_VERSION, lit(1))
        .select(header.map(col): _*)
        .write
        .mode(saveMode)

      val writer = if (partitionKeys.isDefined) w.partitionBy(partitionKeys.get: _*) else w

      writer.parquet(s"$BASE_URI$tablePath")
      writer.parquet(currentPath)

      in.unpersist()
    }
  }

  def writeChangeTable(fs: FileSystem, df: DataFrame, header: List[String], fileName: String, daysAgo: Int) {
    // remove partitions > daysAgo old
    try { removeParts(fs, fileName, daysAgo) } catch { case _: Throwable => } //ignore error
    df
      .select(header.map(col): _*)
      .write
      .mode(SaveMode.Append)
      .partitionBy(META_PROCESS_DATE)
      .parquet(fileName)
  }

  /**
    * Remove date partitioned files more than daysAgo old.
    *
    * @param fs      Hadoop FileSystem
    * @param uri     String
    * @param daysAgo Int
    */
  def removeParts(fs: FileSystem, uri: String, daysAgo: Int) {
    val datePattern = """.*(\d{4}-\d{2}-\d{2})$""".r
    val addedPath = new Path(uri)
    val parts = fs.listFiles(addedPath, false)
    while (parts.hasNext) {
      val part = parts.next().getPath
      val date = for (m <- datePattern findFirstMatchIn part.getName) yield m group 1
      val partDate = new LocalDateTime(date)
      if (partDate < LocalDateTime.now - daysAgo.days) {
        fs.delete(new Path(s"$uri/$META_PROCESS_DATE=$date"), true)
      }
    }
  }

  def registerCustomers(df: DataFrame, idField: String, idType: String) {
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    val path = new Path(CUSTOMER_HUB_PATH)
    val sqlContext = df.sqlContext
    sqlContext.udf.register("hashKey", hashKey(_: String))
    df.registerTempTable("imported")
    if (fs.exists(path)) {
      val existing = sqlContext.read.load(s"$BASE_URI$CUSTOMER_HUB_PATH")
      existing.registerTempTable("existing")
      val newCustomers = sqlContext.sql(
        s"""
           |select hashKey(concat('$idType',i.$idField)) as entity_id
           |,i.$idField as customer_id
           |,'$idType' as customer_id_type
           |,current_timestamp() as process_time
           |from imported i
           |left join existing e on e.customer_id = i.$idField and e.customer_id_type = '$idType'
           |where e.entity_id is null
        """.stripMargin)
      newCustomers.write
        .partitionBy("customer_id_type")
        .mode(SaveMode.Append)
        .parquet(s"$BASE_URI$CUSTOMER_HUB_PATH")
    } else {
      val customers = sqlContext.sql(
        s"""
           |select hashKey(concat('$idType',i.$idField)) as entity_id
           |,i.$idField as customer_id
           |,'$idType' as customer_id_type
           |,current_timestamp() as process_time
           |from imported i
        """.stripMargin)
      customers.write
        .partitionBy("customer_id_type")
        .parquet(s"$BASE_URI$CUSTOMER_HUB_PATH")
    }
  }

}

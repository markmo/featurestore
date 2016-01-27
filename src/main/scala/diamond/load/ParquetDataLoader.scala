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
  * Created by markmo on 23/01/2016.
  */
class ParquetDataLoader extends DataLoader {

  val FILE_EXT = ".parquet"
  val FILE_NEW = s"new$FILE_EXT"
  val FILE_CHANGED = s"changed$FILE_EXT"
  val FILE_REMOVED = s"removed$FILE_EXT"
  val FILE_META = s"meta$FILE_EXT"
  val FILE_CURRENT = s"current$FILE_EXT"

  val LAYER_IL = "il"

  val CUSTOMER_HUB_PATH = s"/$LAYER_IL/customer_hub$FILE_EXT"

  // TODO
  // Cover each scenario:
  // * Delta - no effective dates
  // * Full - no effective dates
  // * Delta - with effective dates
  // * Full - with effective dates

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

    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    val tablePath = s"/$LAYER_IL/$tableName/$tableName$FILE_EXT"
    val path = new Path(tablePath)
    val sqlContext = df.sqlContext
    val headers: List[String] = META_ENTITY_ID :: satNames ++ List(META_OP, META_VERSION, META_HASHED_VALUE)

    val hi = hashRows(sat, updatedNames)

    if (fs.exists(path)) {

      //val existing = sqlContext.read.load(s"$BASE_URI$tablePath").cache()

      // need to use the __current__ set or join below will match multiple rows
      // TODO
      // if current doesn't exist, create empty DataFrame
      val existing = sqlContext.read.load(s"$BASE_URI/$LAYER_IL/$tableName/$FILE_CURRENT").cache()

      //val he = hashRows(existing, updatedNames)

      // alternative: use pre-hashed values from meta table
      //val he = sqlContext.read.load(s"$BASE_URI/il/$tableName/$FILE_META")

      // or the current table
      val existingHashedValueKey = "existing_hashed_value"
      val he = existing.withColumnRenamed(META_HASHED_VALUE, existingHashedValueKey)

      // with update capability, read would filter on `end_time = '9999-12-31'`
      // to select current records, but end_time is not being updated on old records

      // records in the new set that aren't in the existing set
      val added = hi
        .join(existing, sat(META_ENTITY_ID) === existing(META_ENTITY_ID), "left")
        .where(existing(META_ENTITY_ID).isNull)
        .withColumn(META_OP, lit("I"))
        .withColumn(META_VERSION, lit(1))

      // records in the new set that are also in the existing set
      val updated = hi
        .join(he, META_ENTITY_ID)
        .where(hi(META_HASHED_VALUE) !== he(existingHashedValueKey))
        .withColumn(META_OP, lit("U"))
        .withColumn(META_VERSION, lit(he(META_VERSION) + 1))

      // remove partitions > 3 days old
      val daysAgo = 3

      // adds
      removeParts(fs, s"$BASE_URI/$LAYER_IL/$tableName/$FILE_NEW", daysAgo)

      added
        .select(headers.map(col): _*)
        .write
        .partitionBy(META_PROCESS_DATE)
        .parquet(s"$BASE_URI/$LAYER_IL/$tableName/$FILE_NEW")

      // updates
      removeParts(fs, s"$BASE_URI/$LAYER_IL/$tableName/$FILE_CHANGED", daysAgo)

      updated
        .select(headers.map(col): _*)
        .write
        .partitionBy(META_PROCESS_DATE)
        .parquet(s"$BASE_URI/$LAYER_IL/$tableName/$FILE_CHANGED")

      //val changed = updated.unionAll(added).cache()
      // added + updated = total new ??
      //val changed = sat
      val changed = hi

      var deleted: DataFrame = null

      // if receiving full set
      if (!isDelta) {
        deleted = existing
          .join(changed, existing(META_ENTITY_ID) === changed(META_ENTITY_ID), "left")
          .where(changed(META_ENTITY_ID).isNull)
          .withColumn(META_OP, lit("D"))
          .withColumn(META_VERSION, lit(existing(META_VERSION) + 1))
          .cache()

        // deletes
        removeParts(fs, s"$BASE_URI/$LAYER_IL/$tableName/$FILE_REMOVED", daysAgo)

        deleted
          .select(headers.map(col): _*)
          .write
          .partitionBy(META_PROCESS_DATE)
          .parquet(s"$BASE_URI/$LAYER_IL/$tableName/$FILE_REMOVED")
      }

      val allChanges = if (isDelta) changed else changed.unionAll(deleted)
      allChanges.cache()

      // meta
      allChanges
        .select(META_ENTITY_ID, META_START_TIME, META_END_TIME, META_OP, META_VERSION, META_HASHED_VALUE)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"$BASE_URI/$LAYER_IL/$tableName/$FILE_META")

      // all
      val writer = allChanges
        .select(headers.map(col): _*)
        .write
        .mode(SaveMode.Append)

      if (partitionKeys.isDefined) {
        writer
          .partitionBy(partitionKeys.get :_*)
          .parquet(s"$BASE_URI$tablePath")
      } else {
        writer
          .parquet(s"$BASE_URI$tablePath")
      }

      // snapshot
      val latest: RDD[Row] = existing.unionAll(allChanges)
        .map(row => (row.getAs[String](META_ENTITY_ID), row))
        .reduceByKey((a, b) => if (b.getAs[Int](META_VERSION) > a.getAs[Int](META_VERSION)) b else a)
        .map(_._2)

      val latestDF = sqlContext.createDataFrame(latest, existing.schema)

      latestDF
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"$BASE_URI/$LAYER_IL/$tableName/$FILE_CURRENT")

      allChanges.unpersist()
      deleted.unpersist()

    } else {
      val writer = hi
        .withColumn(META_OP, lit("I"))
        .withColumn(META_VERSION, lit(1))
        .select(headers.map(col): _*)
        .write
        .mode(SaveMode.Append)

      if (partitionKeys.isDefined) {
        writer
          .partitionBy(partitionKeys.get: _*)
          .parquet(s"$BASE_URI$tablePath")
      } else {
        writer
          .parquet(s"$BASE_URI$tablePath")
      }
    }
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

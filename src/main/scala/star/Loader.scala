package star

import java.sql.Timestamp
import java.util.{Calendar, Date}

import diamond.utility.hashFunctions._
import diamond.utility.udfs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import star.io.{JdbcReader, HiveWriter}

/**
  * Created by markmo on 12/03/2016.
  */
class Loader(implicit val sqlContext: SQLContext, implicit val conf: StarConfig) {

  import conf._

  val sc = sqlContext.sparkContext
  val reader = new JdbcReader
  val writer = new HiveWriter

  def dimensionalize(processId: String, userId: String): Unit = {
    tables.foreach { source =>
      val df = reader.read(source)
      val splits = source.split("\\.")
      val tableName = splits(1).replaceAll("\\s+", "_").toLowerCase
      writer.write(df, s"$schema.$tableName")
      writer.writeSample(df, s"$schema.$tableName", maxSize = sampleSize)
      if (dims.contains(source)) {
        dims(source).foreach { case (keys: List[String], attrs: List[String]) =>
          loadDim(df, keys, attrs, processId, userId, source)
        }
        if (facts.contains(source)) {
          loadFacts(df, facts(source), dims(source).map(_._1), processId, userId, source)
        }
      }
    }
  }

  def loadFacts(df: DataFrame,
                factFields: List[String],
                dims: List[List[String]],
                processId: String,
                userId: String,
                source: String
               ): Unit = {
    val fkLookups = sc.broadcast(dims.map { dimFields =>
      val tableName = s"$schema.dim_" + dimFields.head.toLowerCase
      (tableName,
        sqlContext
          .sql(s"select id, hashed_key from $tableName")
          .map(r => (r(1), r(0)))
          .collectAsMap
        )
    }.toMap)
    val rows = df.rdd.map { row =>
      dims.foldLeft(row) {
        case (r: Row, dimFields: List[String]) => {
          val key = dimFields.map(field => {
            val idx = row.fieldIndex(field)
            if (r.isNullAt(idx)) {
              unknown
            } else {
              r.getString(idx)
            }
          }).mkString("")
          val hashedKey = hashKey(key)
          val tableName = s"$schema.dim_" + dimFields.head.toLowerCase
          val fk = fkLookups.value.get(tableName).get(hashedKey)
          Row.fromSeq(r.toSeq :+ fk)
        }
      }
    }
    val withRels = sqlContext.createDataFrame(rows,
      StructType(df.schema.fields ++
        dims.map(dimFields => StructField("fk_" + dimFields.head.toLowerCase, LongType))
      ))
    val out = withRels.select(factFields.map(col) ++
      dims.map(dimFields => "fk_" + dimFields.head.toLowerCase).map(col): _*)
    val renamed = factFields.foldLeft(out) {
      case (d, field) => d.withColumnRenamed(field, field.toLowerCase)
    }
    val splits = source.split("\\.")
    val name = splits(1).replaceAll("\\s+", "_").toLowerCase
    writer.write(renamed, s"$schema.fact_$name")
  }

  def loadDim(df: DataFrame,
              dimFields: List[String],
              attrs: List[String],
              processId: String,
              userId: String,
              source: String
             ): Unit = {
    val distinct = if (attrs.isEmpty) {
      df.select(dimFields.map(col): _*)
        .na.fill(unknown)
        .distinct()
    } else {
      val rows = df
        .select(dimFields.map(col) ++ attrs.map(col): _*)
        .na.fill(unknown)
        .map(row => (dimFields.map(row.getAs[String]).mkString("|"), row))
        .reduceByKey((a, b) => a)
        .map(_._2)

      val schema = StructType(
        dimFields.map(field => StructField(field, StringType)) ++
          attrs.map(field => StructField(field, StringType))
      )
      sqlContext.createDataFrame(rows, schema)
    }
    if (distinct.count() > 0) {
      val projection = dimFields ++ attrs
      val dimDF = distinct
        .select(projection.map(col(_).cast(StringType)): _*)
        .withColumn("start_time", current_timestamp().cast(TimestampType))
        .withColumn("end_time", lit(defaultEndDate).cast(TimestampType))
        .withColumn("process_id", lit(processId))
        .withColumn("process_ts", current_timestamp().cast(TimestampType))
        .withColumn("user_id", lit(userId))
        .withColumn("source", lit(source))
        .withColumn("hashed_key", hashKeyUDF(concat(dimFields.map(col): _*)))
        .withColumn("hashed_value", hashKeyUDF(concat(attrs.map(col): _*)))
        .withColumn("rectype", lit("I"))
        .withColumn("version", lit(1))

      val renamed = projection.foldLeft(dimDF) {
        case (d: DataFrame, field: String) => d.withColumnRenamed(field, field.toLowerCase)
      }
      val withId = renamed.rdd.zipWithUniqueId.map {
        case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)
      }
      val withIdDF = sqlContext.createDataFrame(withId, StructType(StructField("id", LongType, nullable = true) +: dimDF.schema.fields))
      writeDim(withIdDF, dimFields, attrs, processId, userId)
    }
  }

  def loadDim(df: DataFrame,
              dimField: String,
              attrs: List[String],
              processId: String,
              userId: String,
              source: String
             ): Unit = {
    val distinct = if (attrs.isEmpty) {
      df.select(dimField)
        .na.fill(unknown)
        .distinct()
    } else {
      val rows = df
        .select(col(dimField) :: attrs.map(col): _*)
        .map(row => (dimField, row))
        .reduceByKey((a, b) => a)
        .map(_._2)

      val schema = StructType(
        StructField(dimField, StringType) ::
          attrs.map(field => StructField(field, StringType))
      )
      sqlContext.createDataFrame(rows, schema)
    }
    if (distinct.count > 0) {
      val dimDF = distinct
        .select(col(dimField).cast(StringType) :: attrs.map(col): _*)
        .withColumn("start_time", current_timestamp().cast(TimestampType))
        .withColumn("end_time", lit(defaultEndDate).cast(TimestampType))
        .withColumn("process_id", lit(processId))
        .withColumn("process_ts", current_timestamp())
        .withColumn("user_id", lit(userId))
        .withColumn("source", lit(source))
        .withColumn("hashed_key", hashKeyUDF(col(dimField)))
        .withColumn("hashed_value", hashKeyUDF(concat(attrs.map(col): _*)))
        .withColumn("rectype", lit("I"))
        .withColumn("version", lit(1))

      val renamed = attrs.foldLeft(dimDF) {
        case (d: DataFrame, field: String) => d.withColumnRenamed(field, field.toLowerCase)
      }
      val withId = renamed.rdd.zipWithUniqueId.map {
        case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)
      }
      val withIdDF = sqlContext.createDataFrame(withId, StructType(StructField("id", LongType, nullable = true) +: dimDF.schema.fields))
      writeDim(withIdDF, List(dimField), attrs, processId, userId)
    }
  }

  def writeDim(df: DataFrame,
               dimFields: List[String],
               attrs: List[String],
               processId: String,
               userId: String
              ): Unit = {
    val dimName = "dim_" + dimFields.head.toLowerCase
    val tableName = s"$schema.$dimName"
    val fields = df.schema.fieldNames.toList
    val tableExist = try {
      sqlContext.sql(s"select count(*) from $tableName").collect
      true
    } catch {
      case _: Throwable => false
    }
    val ex = if (tableExist) {
      val existing = sqlContext.sql(s"select * from $tableName")
      existing
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"$parquetPath/$dimName.prev")

      sqlContext.read
        .load(s"$parquetPath/$dimName.prev")
        .filter(s"end_time = '$defaultEndDate'")

    } else {
      val cal = Calendar.getInstance
      cal.set(9999, 11, 1)
      val defaultEndDate = cal.getTime
      val hashedKey = hashKey(unknown)
      val fields = -1L :: dimFields.map(_ => unknown) ++
        attrs.map(_ => unknown) ++ List(
        new Timestamp(new Date().getTime),
        new Timestamp(defaultEndDate.getTime),
        processId,
        new Timestamp(new Date().getTime),
        userId,
        "default",
        hashedKey,
        hashedKey,
        "I",
        1
      )
      val defaultRows = List(Row.fromSeq(fields))
      val defaultRDD = sc.parallelize(defaultRows)
      sqlContext.createDataFrame(defaultRDD, df.schema)
    }
    val seed = {
      val n = ex.agg(max(col("id"))).first().getAs[Long](0)
      if (n == 0) 1L else n + 1
    }
    val in = df.withColumn("id", col("id") + lit(seed))

    val newRecords = in
      .join(ex, in("id") === ex("id"), "left_outer")
      .where(ex("id").isNull)
      .select(fields.map(in(_)): _*)

    val changed = in
      .join(ex, "id")
      .where(in("hashed_value") !== ex("hashed_value"))

    val updatesNew = changed
      .select(ex("version").as("old_version") :: fields.map(in(_)): _*)
      .withColumn("rectype", lit("U"))
      .withColumn("version", col("old_version") + lit(1))
      .drop("old_version")

    val updatesExisting = changed
      .select(in("start_time").as("new_start_time") :: fields.map(in(_)): _*)
      .withColumn("rectype", lit("U"))
      .withColumn("end_time", col("new_start_time"))
      .drop("new_start_time")

    val all = updatesExisting.unionAll(updatesNew).unionAll(newRecords)

    val out = ex
      .join(all, ex("id") === all("id") && ex("version") === all("version"), "left_outer")
      .where(all("id") === 0)
      .select(fields.map(ex(_)): _*)
      .unionAll(all)

    writer.write(out, tableName)
  }
}

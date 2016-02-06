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
  val FILE_PREV = s"prev$FILE_EXT"

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
                    writeChangeTables: Boolean = false) {

    if (isDelta && overwrite) throw sys.error("isDelta and overwrite options are mutually exclusive")
    val renamed = newNames.foldLeft(df)({
      case (d, (oldName, newName)) => d.withColumnRenamed(oldName, newName)
    })
    val baseNames = renamed.schema.fieldNames.toList diff idFields
    val t = renamed
      .withColumn(META_ENTITY_ID, hashKeyUDF(concat(lit(idType), concat(idFields.map(col): _*))))
      .withColumn(META_START_TIME, current_timestamp().cast(TimestampType))
      .withColumn(META_END_TIME, lit(META_OPEN_END_DATE_VALUE).cast(TimestampType))
      .withColumn(META_SOURCE, lit(source))
      .withColumn(META_PROCESS_TYPE, lit(processType))
      .withColumn(META_PROCESS_ID, lit(processId))
      .withColumn(META_PROCESS_DATE, current_date())
      .withColumn(META_USER_ID, lit(userId))
      .withColumn(META_HASHED_VALUE, fastHashUDF(concat(baseNames.map(col): _*)))
      .withColumn(META_VALID_START_TIME_FIELD, lit(null))
      .withColumn(META_VALID_END_TIME_FIELD, lit(null))
      .withColumn(META_VALID_START_TIME, current_timestamp().cast(TimestampType))
      .withColumn(META_VALID_END_TIME, lit(META_OPEN_END_DATE_VALUE).cast(TimestampType))
      .withColumn(META_DELETE_INDICATOR_FIELD, lit(null))

    val t1 = if (validStartTimeField.isDefined && validEndTimeField.isDefined) {
      t
        .withColumn(META_VALID_START_TIME_FIELD, lit(validStartTimeField.get._1))
        .withColumn(META_VALID_END_TIME_FIELD, lit(validEndTimeField.get._1))
        .withColumn(META_VALID_START_TIME, convertStringToTimestampUDF(col(validStartTimeField.get._1), lit(validStartTimeField.get._2)))
        .withColumn(META_VALID_END_TIME, convertStringToTimestampUDF(col(validEndTimeField.get._1), lit(validEndTimeField.get._2)))
    } else {
      t
    }

    val in = if (deleteIndicatorField.isDefined) {
      t1.withColumn(META_DELETE_INDICATOR_FIELD, lit(deleteIndicatorField.get._1))
    } else {
      t1
    }

    // add column headers for process metadata
    val names = META_ENTITY_ID :: baseNames ++
      List(META_VALID_START_TIME_FIELD, META_VALID_END_TIME_FIELD,
        META_VALID_START_TIME, META_VALID_END_TIME,
        META_START_TIME, META_END_TIME,
        META_DELETE_INDICATOR_FIELD, META_PROCESS_DATE, META_HASHED_VALUE)

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
        .join(ex, in(META_ENTITY_ID) === ex(META_ENTITY_ID), "left_outer")
        .where(ex(META_ENTITY_ID).isNull)
        .select(names.map(in(_)): _*)
        .withColumn(META_RECTYPE, lit(RECTYPE_INSERT))
        .withColumn(META_VERSION, lit(1))

      // records in the new set that are also in the existing set
      val matched = in
        .join(ex, META_ENTITY_ID)
        .where(in(META_HASHED_VALUE) === ex(META_HASHED_VALUE))
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
              .join(in, ex(META_ENTITY_ID) === in(META_ENTITY_ID), "left_outer")
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
          val cols =
            in(META_START_TIME).as("new_start_time") ::
              in(META_ENTITY_ID) :: (header diff List(META_ENTITY_ID)).map(ex(_))
          val updatesExisting = changes
            .select(cols: _*)
            .withColumn(META_RECTYPE, lit(RECTYPE_UPDATE))
            .withColumn(META_END_TIME, col("new_start_time"))
            .select(header.map(col): _*)

          updatesNew.unionAll(updatesExisting)
        } else {
          updatesNew
        }

      val all = deletes match {
        case Some(d) => inserts.unionAll(updates).unionAll(d)
        case None => inserts.unionAll(updates)
      }

      val main =
        if (overwrite) {
          val ex = sqlContext.read.load(s"$BASE_URI$tablePath")
          val prevPath = s"$BASE_URI/$LAYER_IL/$tableName/$FILE_PREV"
          ex.write.mode(SaveMode.Overwrite).parquet(prevPath)
          val prev = sqlContext.read.load(prevPath)
          prev
            .join(all, prev(META_ENTITY_ID) === all(META_ENTITY_ID) && prev(META_VERSION) === all(META_VERSION), "left_outer")
            .where(all(META_ENTITY_ID).isNull)
            .select(header.map(prev(_)): _*)
            .unionAll(all)
        } else {
          all
        }

      main.cache()

      val writer =
        if (partitionKeys.isDefined) {
          main.write.mode(saveMode).partitionBy(partitionKeys.get: _*)
        } else {
          main.write.mode(saveMode)
        }

      writer.parquet(s"$BASE_URI$tablePath")

      // write snapshot
      val latest: RDD[Row] = ex.unionAll(main)
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

      main.unpersist()
      matched.unpersist()
      ex.unpersist()

    } else {
      val out = idFields.foldLeft(in)({
        case (d, idField) => d.drop(idField)
      })
      out.cache()
      val w = out
        .withColumn(META_RECTYPE, lit(RECTYPE_INSERT))
        .withColumn(META_VERSION, lit(1))
        .select(header.map(col): _*)
        .write
        .mode(saveMode)

      val writer = if (partitionKeys.isDefined) w.partitionBy(partitionKeys.get: _*) else w

      writer.parquet(s"$BASE_URI$tablePath")
      writer.parquet(currentPath)

      out.unpersist()
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
               overwrite: Boolean = false) {

    if (isDelta && overwrite) throw sys.error("isDelta and overwrite options are mutually exclusive")
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    val tn = if (tableName.isDefined) tableName.get else s"${entityType1.toLowerCase}_${entityType2.toLowerCase}_link"
    val path = s"/$LAYER_IL/$tn$FILE_EXT"
    val currentPath = s"$BASE_URI/$LAYER_IL/$tn/$FILE_CURRENT"
    val sqlContext = df.sqlContext
    sqlContext.udf.register("hashKey", hashKey(_: String))
    sqlContext.udf.register("convertStringToTimestamp", convertStringToTimestamp(_: String, _: String))
    df.registerTempTable("imported")
    val idCols1 = idFields1.map(f => s"i.$f").mkString(",")
    val idCols2 = idFields2.map(f => s"i.$f").mkString(",")
    val (validStartTimeFieldLit, validEndTimeFieldLit, validStartTimeExpr, validEndTimeExpr) =
      if (validStartTimeField.isDefined && validEndTimeField.isDefined) {
        (
          s"'${validStartTimeField.get._1}'",
          s"'${validEndTimeField.get._1}'",
          s"convertStringToTimestamp(i.${validStartTimeField.get._1}, '${validStartTimeField.get._2}'",
          s"convertStringToTimestamp(i.${validEndTimeField.get._1}, '${validEndTimeField.get._2}'"
          )
      } else {
        ("NULL", "NULL", "current_timestamp()", s"'$META_OPEN_END_DATE_VALUE'")
      }

    val deleteIndicatorFieldName =
      if (deleteIndicatorField.isDefined) {
        s"'${deleteIndicatorField.get._1}'"
      } else {
        "NULL"
      }

    val sql =
      s"""
         |select hashKey(concat('$idType1',$idCols1)) as entity_id_1
         |,hashKey(concat('$idType2',$idCols2)) as entity_id_2
         |,'$entityType1' as entity_type_1
         |,'$entityType2' as entity_type_2
         |,current_timestamp() as start_time
         |,'$META_OPEN_END_DATE_VALUE' as end_time
         |,'$source' as source
         |,'$processType' as process_type
         |,'$processId' as process_id
         |,current_date() as process_date
         |,'$userId' as user_id
         |,$validStartTimeFieldLit as valid_start_time_field
         |,$validEndTimeFieldLit as valid_end_time_field
         |,$validStartTimeExpr as valid_start_time
         |,$validEndTimeExpr as valid_end_time
         |,$deleteIndicatorFieldName as delete_indicator_field
         |,'$RECTYPE_INSERT' as rectype
         |,1 as version
         |from imported i
      """.stripMargin

    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.Append

    if (fs.exists(new Path(path))) {
      val existing = sqlContext.read.load(s"$BASE_URI$path")
      existing.registerTempTable("existing")
      val sqlNewLinks =
        s"""
           |$sql
           |left join existing e on e.entity_id_1 = hashKey(concat('$idType1',$idCols1))
           |and e.entity_id_2 = hashKey(concat('$idType2',$idCols2))
           |where e.entity_id is null
        """.stripMargin

      val validStartExpr =
        if (validStartTimeField.isDefined && validEndTimeField.isDefined) {
          validStartTimeExpr
        } else {
          s"e.valid_start_time"
        }

      val selectExisting =
        s"""
           |select e.entity_id_1
           |,e.entity_id_2
           |,e.entity_type_1
           |,e.entity_type_2
           |,e.start_time
           |,current_timestamp() as end_time
           |,'$source' as source
           |,'$processType' as process_type
           |,'$processId' as process_id
           |,current_date() as process_date
           |,'$userId' as user_id
           |,$validStartTimeFieldLit as valid_start_time_field
           |,$validEndTimeFieldLit as valid_end_time_field
           |,$validStartExpr as valid_start_time
           |,$validEndTimeExpr as valid_end_time
           |,$deleteIndicatorFieldName as delete_indicator_field
           |,'$RECTYPE_DELETE' as rectype
           |,e.version + 1 as version
           |from existing e
           |inner join
           |(select entity_id, max(version) as max_version
           |from existing
           |group by entity_id) e1 on e1.entity_id = e.entity_id and e1.version = e.version
         """.stripMargin

      val all =
        if (deleteIndicatorField.isDefined) {
          val delIndField = deleteIndicatorField.get._1
          val delIndFieldVal = deleteIndicatorField.get._2.toString
          val delIndFieldLit = if (isNumber(delIndFieldVal)) delIndFieldVal else s"'$delIndFieldVal'"

          // inserts
          val inserts = sqlContext.sql(
            s"""
               |$sqlNewLinks
               |and i.$delIndField <> $delIndFieldLit
            """.stripMargin)

          if (overwrite) {
            inserts
          } else {
            // union deletes
            inserts.unionAll(sqlContext.sql(
              s"""
                 |$selectExisting
                 |join imported i on e.entity_id_1 = hashKey(concat('$idType1',$idCols1))
                 |and e.entity_id_2 = hashKey(concat('$idType2',$idCols2))
                 |where i.$delIndField = $delIndFieldLit
              """.stripMargin))
          }
        } else if (!isDelta) {
          // inserts
          val inserts = sqlContext.sql(sqlNewLinks)
          if (overwrite) {
            inserts
          } else {
            // union deletes
            inserts.unionAll(sqlContext.sql(
              s"""
                 |$selectExisting
                 |left join imported i on e.entity_id_1 = hashKey(concat('$idType1',$idCols1))
                 |and e.entity_id_2 = hashKey(concat('$idType2',$idCols2))
                 |where i.entity_id is null
              """.stripMargin))
          }
        } else {
          // inserts
          sqlContext.sql(sqlNewLinks)
        }

      all.write
        .partitionBy("entity_type_1", "entity_type_2")
        .mode(saveMode)
        .parquet(s"$BASE_URI$path")

      // write snapshot
      val latest: RDD[Row] = all
        .map(row => (row.getAs[String](META_ENTITY_ID), row))
        .reduceByKey((a, b) => if (b.getAs[Int](META_VERSION) > a.getAs[Int](META_VERSION)) b else a)
        .map(_._2)

      val latestDF = sqlContext.createDataFrame(latest, all.schema)

      latestDF
        .write
        .mode(SaveMode.Overwrite)
        .parquet(currentPath)

    } else {
      val links = sqlContext.sql(sql)

      links.write
        .partitionBy("entity_type_1", "entity_type_2")
        .mode(saveMode)
        .parquet(s"$BASE_URI$path")
    }
  }

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
              overwrite: Boolean = false) {

    if (isDelta && overwrite) throw sys.error("isDelta and overwrite options are mutually exclusive")
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    val tn = if (tableName.isDefined) tableName.get else s"${entityType.toLowerCase}_hub"
    val path = s"/$LAYER_IL/$tn$FILE_EXT"
    val currentPath = s"$BASE_URI/$LAYER_IL/$tn/$FILE_CURRENT"
    val sqlContext = df.sqlContext
    sqlContext.udf.register("hashKey", hashKey(_: String))
    sqlContext.udf.register("convertStringToTimestamp", convertStringToTimestamp(_: String, _: String))
    df.registerTempTable("imported")
    val idCols = idFields.map(f => s"i.$f").mkString(",")
    val (validStartTimeFieldLit, validEndTimeFieldLit, validStartTimeExpr, validEndTimeExpr) =
      if (validStartTimeField.isDefined && validEndTimeField.isDefined) {
        (
          s"'${validStartTimeField.get._1}'",
          s"'${validEndTimeField.get._1}'",
          s"convertStringToTimestamp(i.${validStartTimeField.get._1}, '${validStartTimeField.get._2}'",
          s"convertStringToTimestamp(i.${validEndTimeField.get._1}, '${validEndTimeField.get._2}'"
          )
      } else {
        ("NULL", "NULL", "current_timestamp()", s"'$META_OPEN_END_DATE_VALUE'")
      }

    val deleteIndicatorFieldName =
      if (deleteIndicatorField.isDefined) {
        s"'${deleteIndicatorField.get._1}'"
      } else {
        "NULL"
      }

    val sql =
      s"""
         |select hashKey(concat('$idType',$idCols)) as entity_id
         |,'$entityType' as entity_type
         |,$idCols
         |,'$idType' as id_type
         |,current_timestamp() as start_time
         |,'$META_OPEN_END_DATE_VALUE' as end_time
         |,'$source' as source
         |,'$processType' as process_type
         |,'$processId' as process_id
         |,current_date() as process_date
         |,'$userId' as user_id
         |,$validStartTimeFieldLit as valid_start_time_field
         |,$validEndTimeFieldLit as valid_end_time_field
         |,$validStartTimeExpr as valid_start_time
         |,$validEndTimeExpr as valid_end_time
         |,$deleteIndicatorFieldName as delete_indicator_field
         |,'$RECTYPE_INSERT' as rectype
         |,1 as version
         |from imported i
      """.stripMargin

    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.Append

    if (fs.exists(new Path(path))) {
      val existing = sqlContext.read.load(s"$BASE_URI$path")
      existing.registerTempTable("existing")
      val joinPredicates = idFields.map(f => s"e.$f = i.$f").mkString(" and ")
      val sqlNewEntities =
        s"""
           |$sql
           |left join existing e on $joinPredicates and e.id_type = '$idType'
           |where e.entity_id is null
        """.stripMargin

      val validStartExpr =
        if (validStartTimeField.isDefined && validEndTimeField.isDefined) {
          validStartTimeExpr
        } else {
          s"e.valid_start_time"
        }

      val selectExisting =
        s"""
           |select e.entity_id
           |,e.entity_type
           |,e.start_time
           |,current_timestamp() as end_time
           |,'$source' as source
           |,'$processType' as process_type
           |,'$processId' as process_id
           |,current_date() as process_date
           |,'$userId' as user_id
           |,$validStartTimeFieldLit as valid_start_time_field
           |,$validEndTimeFieldLit as valid_end_time_field
           |,$validStartExpr as valid_start_time
           |,$validEndTimeExpr as valid_end_time
           |,$deleteIndicatorFieldName as delete_indicator_field
           |,'$RECTYPE_DELETE' as rectype
           |,e.version + 1 as version
           |from existing e
           |inner join
           |(select entity_id, max(version) as max_version
           |from existing
           |group by entity_id) e1 on e1.entity_id = e.entity_id and e1.version = e.version
        """.stripMargin

      val all =
        if (deleteIndicatorField.isDefined) {
          val delIndField = deleteIndicatorField.get._1
          val delIndFieldVal = deleteIndicatorField.get._2.toString
          val delIndFieldLit = if (isNumber(delIndFieldVal)) delIndFieldVal else s"'$delIndFieldVal'"

          // inserts
          val inserts = sqlContext.sql(
            s"""
               |$sqlNewEntities
               |and i.$delIndField <> $delIndFieldLit
            """.stripMargin)

          if (overwrite) {
            inserts
          } else {
            // union deletes
            inserts.unionAll(sqlContext.sql(
              s"""
                 |$selectExisting
                 |join imported i on $joinPredicates
                 |where e.id_type = '$idType'
                 |where i.$delIndField = $delIndFieldLit
              """.stripMargin))
          }
        } else if (!isDelta) {
          // inserts
          val inserts = sqlContext.sql(sqlNewEntities)
          if (overwrite) {
            inserts
          } else {
            // union deletes
            inserts.unionAll(sqlContext.sql(
              s"""
                 |$selectExisting
                 |left join imported i on $joinPredicates
                 |where e.id_type = '$idType'
                 |and i.entity_id is null
              """.stripMargin))
          }
        } else {
          // inserts
          sqlContext.sql(sqlNewEntities)
        }

      all.write
        .partitionBy("id_type")
        .mode(saveMode)
        .parquet(s"$BASE_URI$path")

      // write snapshot
      val latest: RDD[Row] = all
        .map(row => (row.getAs[String](META_ENTITY_ID), row))
        .reduceByKey((a, b) => if (b.getAs[Int](META_VERSION) > a.getAs[Int](META_VERSION)) b else a)
        .map(_._2)

      val latestDF = sqlContext.createDataFrame(latest, all.schema)

      latestDF
        .write
        .mode(SaveMode.Overwrite)
        .parquet(currentPath)

    } else {
      val entities = sqlContext.sql(sql)

      entities.write
        .partitionBy("id_type")
        .mode(saveMode)
        .parquet(s"$BASE_URI$path")
    }
  }

  def registerCustomers(df: DataFrame, idFields: List[String], idType: String, processId: String) {
    val fs = FileSystem.get(new URI(BASE_URI), new Configuration())
    val path = new Path(CUSTOMER_HUB_PATH)
    val sqlContext = df.sqlContext
    sqlContext.udf.register("hashKey", hashKey(_: String))
    df.registerTempTable("imported")

    val idCols =
      if (idFields.length == 1) {
        s"i.${idFields.head} as customer_id"
      } else {
        idFields.map(f => s"i.$f").mkString(",")
      }

    val sql =
      s"""
         |select hashKey(concat('$idType',$idCols)) as entity_id
         |,$idCols
         |,'$idType' as customer_id_type
         |,current_timestamp() as start_time
         |,'$META_OPEN_END_DATE_VALUE' as end_time
         |,'$processId' as process_id
         |,current_date() as process_date
         |,1 as version
         |from imported i
      """.stripMargin

    if (fs.exists(path)) {
      val existing = sqlContext.read.load(s"$BASE_URI$CUSTOMER_HUB_PATH")
      existing.registerTempTable("existing")

      val joinPredicates =
        if (idFields.length == 1) {
          s"e.customer_id = i.${idFields.head}"
        } else {
          idFields.map(f => s"e.$f = i.$f").mkString(" and ")
        }

      val newCustomers = sqlContext.sql(
        s"""
           |$sql
           |left join existing e on $joinPredicates and e.customer_id_type = '$idType'
           |where e.entity_id is null
        """.stripMargin)

      newCustomers.write
        .partitionBy("customer_id_type")
        .mode(SaveMode.Append)
        .parquet(s"$BASE_URI$CUSTOMER_HUB_PATH")

    } else {
      val customers = sqlContext.sql(sql)

      customers.write
        .partitionBy("customer_id_type")
        .parquet(s"$BASE_URI$CUSTOMER_HUB_PATH")
    }
  }

  def writeChangeTable(fs: FileSystem, df: DataFrame, header: List[String], fileName: String, daysAgo: Int) {
    // remove partitions > daysAgo old
    try {
      removeParts(fs, fileName, daysAgo)
    } catch {
      case _: Throwable =>
    } //ignore error
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

}

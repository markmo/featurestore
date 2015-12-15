package diamond.transformation

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

/**
  * Created by markmo on 12/12/2015.
  */
object schemas {

  val inputSchema = StructType(Seq(
    StructField("entityIdType", StringType),
    StructField("entityId", StringType),
    StructField("attribute", StringType),
    StructField("ts", StringType),
    StructField("namespace", StringType),
    StructField("value", StringType, nullable = true),
    StructField("properties", StringType, nullable = true),
    StructField("processId", StringType),
    StructField("processTime", StringType),
    StructField("version", IntegerType)
  ))

  val formattedSchema = StructType(Seq(
    StructField("entity", StringType),
    StructField("attribute", StringType),
    StructField("ts", TimestampType),
    StructField("namespace", StringType),
    StructField("value", StringType, nullable = true),
    StructField("properties", StringType, nullable = true),
    StructField("processId", StringType),
    StructField("processTime", TimestampType),
    StructField("version", IntegerType)
  ))

}

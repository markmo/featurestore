package diamond.io

import diamond.transformation.TransformationContext
import diamond.transformation.row.RowTransformation._
import diamond.transformation.sql.NamedSQLTransformation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, DataFrame}

/**
  * Created by markmo on 9/01/2016.
  */
trait Source {

  val sqlContext: SQLContext

  def apply(ctx: TransformationContext): DataFrame

}

case class CSVSource(sqlContext: SQLContext) extends Source {

  /**
    * Source params are supplied to the context:
    * * header - "true"|"false"
    * * schema StructType
    * * in_path - of CSV file to read in
    *
    * @param ctx TransformationContext
    * @return DataFrame
    */
  def apply(ctx: TransformationContext): DataFrame = {
    var reader = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", ctx.getOrElse("header", "false").asInstanceOf[String])

    if (ctx.contains(SCHEMA_KEY)) {
      reader = reader.schema(ctx(SCHEMA_KEY).asInstanceOf[StructType])
    }

    reader.load(ctx("in_path").asInstanceOf[String])
  }

}

case class ParquetSource(sqlContext: SQLContext) extends Source {

  /**
    * Source params are supplied to the context:
    * * in_path - of parquet file
    *
    * @param ctx TransformationContext
    * @return DataFrame
    */
  def apply(ctx: TransformationContext): DataFrame =
    sqlContext.read.parquet(ctx("in_path").asInstanceOf[String])

}

case class SQLSource(sqlContext: SQLContext) extends Source {

  /**
    * Source params are supplied to the context:
    * * propsPath - path to the SQL properties file
    * * name - named SQL query
    *
    * @param ctx TransformationContext
    * @return DataFrame
    */
  def apply(ctx: TransformationContext): DataFrame = {
    val transform = new NamedSQLTransformation(ctx("propsPath").asInstanceOf[String], ctx("name").asInstanceOf[String])
    transform(sqlContext)
  }

}
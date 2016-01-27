package diamond.io

import diamond.transform.TransformationContext
import diamond.transform.row.RowTransformation._
import diamond.transform.sql.{NamedSQLTransformation, SQLFileTransformation, SQLTransformation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by markmo on 9/01/2016.
  */
trait Source {

  val sqlContext: SQLContext

  def apply(ctx: TransformationContext): DataFrame

}

/**
  * Loads a DataFrame from a CSV file.
  *
  * @param sqlContext SQLContext
  */
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

/**
  * Loads a DataFrame from a Parquet file.
  *
  * @param sqlContext SQLContext
  */
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

/**
  * Loads a DataFrame using Spark SQL given a named query from a SQL configuration file.
  *
  * @param sqlContext SQLContext
  */
case class NamedSQLSource(sqlContext: SQLContext) extends Source {

  /**
    * Source params are supplied to the context:
    * * propsPath - path to the SQL properties file
    * * name - named SQL query
    * * sqlparams - Map of parameters
    *
    * @param ctx TransformationContext
    * @return DataFrame
    */
  def apply(ctx: TransformationContext): DataFrame = {
    val transform = new NamedSQLTransformation(ctx("propsPath").asInstanceOf[String], ctx("name").asInstanceOf[String], ctx("sqlparams").asInstanceOf[Map[String, String]])
    transform(sqlContext)
  }

}

/**
  * Loads a DataFrame using Spark SQL given a SQL statement.
  *
  * @param sqlContext SQLContext
  */
case class SQLSource(sqlContext: SQLContext) extends Source {

  /**
    * Source params are supplied to the context:
    * * sql - SQL statement
    * * sqlparams - Map of parameters
    *
    * @param ctx TransformationContext
    * @return DataFrame
    */
  def apply(ctx: TransformationContext): DataFrame = {
    val transform = new SQLTransformation(ctx("sql").asInstanceOf[String], ctx("sqlparams").asInstanceOf[Map[String, String]])
    transform(sqlContext)
  }

}

/**
  * Loads a DataFrame using Spark SQL given a SQL statement loaded from a file.
  *
  * @param sqlContext SQLContext
  */
case class SQLFileSource(sqlContext: SQLContext) extends Source {

  /**
    * Source params are supplied to the context:
    * * filename - containing SQL
    * * sqlparams - Map of parameters
    *
    * @param ctx TransformationContext
    * @return DataFrame
    */
  def apply(ctx: TransformationContext): DataFrame = {
    val transform = new SQLFileTransformation(ctx("filename").asInstanceOf[String], ctx("sqlparams").asInstanceOf[Map[String, String]])
    transform(sqlContext)
  }

}
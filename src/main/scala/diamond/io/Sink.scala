package diamond.io

import diamond.transform.TransformationContext
import org.apache.spark.sql.{SQLContext, DataFrame}

/**
  * Created by markmo on 10/01/2016.
  */
trait Sink {

  val sqlContext: SQLContext

  def apply(df: DataFrame, ctx: TransformationContext): DataFrame

}

/**
  * Saves a DataFrame to a CSV file.
  *
  * @param sqlContext SQLContext
  */
case class CSVSink(sqlContext: SQLContext) extends Sink {

  /**
    * Sink params are supplied to the context:
    * * header - "true"|"false"
    * * out_path - of CSV file to write to
    *
    * @param df DataFrame
    * @param ctx TransformationContext
    * @return DataFrame
    */
  def apply(df: DataFrame, ctx: TransformationContext): DataFrame = {
    val writer = df.write
      .format("com.databricks.spark.csv")
      .option("header", ctx.getOrElse("header", "false").asInstanceOf[String])
    writer.save(ctx("out_path").asInstanceOf[String])
    df
  }

}

/**
  * Saves a DataFrame to a Parquet file.
  *
  * @param sqlContext SQLContext
  */
case class ParquetSink(sqlContext: SQLContext) extends Sink {

  /**
    * Sink params are supplied to the context:
    * * out_path - of parquet file to write to
    *
    * @param df DataFrame
    * @param ctx TransformationContext
    * @return DataFrame
    */
  def apply(df: DataFrame, ctx: TransformationContext): DataFrame = {
    df.write.save(ctx("out_path").asInstanceOf[String])
    df
  }

}

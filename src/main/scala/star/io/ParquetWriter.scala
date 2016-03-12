package star.io

import org.apache.spark.sql.{DataFrame, SaveMode}
import star.StarConfig

/**
  * Created by markmo on 12/03/2016.
  */
class ParquetWriter(implicit val conf: StarConfig) {

  import conf._

  def write(df: DataFrame, tableName: String): Unit = {
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$parquetPath/$tableName.parquet")
  }

  def writeSample(df: DataFrame, tableName: String, maxSize: Long): Unit = {
    val n = df.count()
    val fraction = if (n < maxSize) {
      1.0
    } else {
      maxSize / n
    }
    df.sample(withReplacement = false, fraction = fraction)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$parquetSamplePath/$tableName.parquet")
  }

}

package star.io

import org.apache.spark.sql.{DataFrame, SaveMode}
import star.StarConfig

/**
  * Created by markmo on 12/03/2016.
  */
class HiveWriter(implicit val conf: StarConfig) extends Writer {

  def write(df: DataFrame, tableName: String): Unit = {
    df.coalesce(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${tableName}_sample")
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
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${tableName}_sample")
  }
}

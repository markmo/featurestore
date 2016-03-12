package star.io

import org.apache.spark.sql.DataFrame
import star.StarConfig

/**
  * Created by markmo on 12/03/2016.
  */
trait Writer {

  val conf: StarConfig

  def write(df: DataFrame, tableName: String): Unit

  def writeSample(df: DataFrame, tableName: String, maxSize: Long): Unit

}

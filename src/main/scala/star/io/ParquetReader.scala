package star.io

import org.apache.spark.sql.SQLContext
import star.StarConfig

/**
  * Created by markmo on 12/03/2016.
  */
class ParquetReader(implicit val sqlContext: SQLContext, implicit val conf: StarConfig) extends Reader {

  def read(source: String) = {
    sqlContext.read.load(source)
  }

}

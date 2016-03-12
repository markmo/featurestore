package star.io

import org.apache.spark.sql.SQLContext
import star.StarConfig

/**
  * Created by markmo on 12/03/2016.
  */
class JdbcReader(implicit val sqlContext: SQLContext, implicit val conf: StarConfig) extends Reader {

  import conf._

  def read(source: String) = {
    sqlContext.read
      .format("jdbc")
      .options(Map(
        "driver" -> env.driver,
        "url" -> env.url,
        "dbtable" -> source
      ))
      .load()
  }

}

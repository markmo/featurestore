package diamond.transformation.sql

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by markmo on 16/12/2015.
  */
class SQLTransformation(sql: String) extends Serializable {

  def apply(sqlContext: SQLContext): DataFrame = sqlContext.sql(sql)

}

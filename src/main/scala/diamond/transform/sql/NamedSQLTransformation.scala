package diamond.transform.sql

import diamond.utility.functions
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by markmo on 16/12/2015.
  */
class NamedSQLTransformation(propsPath: String, name: String, params: Map[String, String]) extends Serializable {

  import functions._

  def apply(sqlContext: SQLContext): DataFrame = {
    val sqlMap = SQLLoader.load(propsPath)
    sqlContext.sql(sqlMap(name).template(params))
  }

}

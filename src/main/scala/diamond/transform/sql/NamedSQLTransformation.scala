package diamond.transform.sql

import diamond.utility.stringFunctions
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by markmo on 16/12/2015.
  *
  * @param propsPath String path to SQL properties file (relative to classpath)
  * @param name String name of query
  * @param params Map[String, String] dynamic variables to pass into query
  */
class NamedSQLTransformation(propsPath: String, name: String, params: Map[String, String]) extends Serializable {

  import stringFunctions._

  def apply(sqlContext: SQLContext): DataFrame = {
    val sqlMap = SQLLoader.load(propsPath)
    sqlContext.sql(sqlMap(name).template(params))
  }

}

package diamond.transformation.sql

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by markmo on 16/12/2015.
  */
class NamedSQLTransformation(propsPath: String, name: String) extends Serializable {

  def apply(sqlContext: SQLContext): DataFrame = {
    val sqlMap = SQLLoader.load(propsPath)
    sqlContext.sql(sqlMap(name))
  }

}

package diamond.transformation.table

import diamond.transformation.TransformationContext
import org.apache.spark.sql.DataFrame

/**
  * Created by markmo on 16/12/2015.
  */
trait SQLTableTransformation extends TableTransformation {

  val sql: String

  val tableName: String

  def apply(df: DataFrame, ctx: TransformationContext): DataFrame = {
    df.registerTempTable(tableName)
    df.sqlContext.sql(sql)
  }

}

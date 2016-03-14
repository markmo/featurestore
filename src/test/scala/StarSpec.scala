import com.typesafe.config.ConfigFactory
import common.utility.stringFunctions._
import star.io.{CSVReader, Reader}
import star.{Loader, StarConfig}

/**
  * Created by markmo on 12/03/2016.
  */
class StarSpec extends UnitSpec {

  val starConf = new StarConfig(ConfigFactory.load("star.conf"))

  @transient var csvReader: Reader = _
  @transient var loader: Loader = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    csvReader = new CSVReader()(sqlContext, starConf)
    loader = new Loader()(sqlContext, starConf)
    sqlContext.sql("create schema if not exists test")
  }

  "Star Loader" should "dimensionalize a denormalized source" in {

    val source = "superstore_sales.csv"
    val path = getClass.getResource(s"base/$source").getPath
    val df = csvReader.read(path)
    val renamed = df.schema.fieldNames.foldLeft(df) {
      case (d, field) => d.withColumnRenamed(field, underscore(field.toLowerCase))
    }
    val (dimFields, attrs) = starConf.dims(source).head
    loader.loadDim(renamed, dimFields, attrs, "test", "test", source)

    sqlContext.sql("select * from test.dim_order_priority").show(10)

    val dimCount = sqlContext.sql("select count(*) from test.dim_order_priority").first()(0)

    dimCount should be (6)
  }

}

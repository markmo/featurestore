import com.typesafe.config.ConfigFactory
import star.io.JdbcReader
import star.{Loader, StarConfig}

/**
  * Created by markmo on 12/03/2016.
  */
class StarSpec extends UnitSpec {

  override implicit val conf = new StarConfig(ConfigFactory.load("star.conf"))

  @transient val jdbcReader = new JdbcReader
  @transient val loader = new Loader

  "Star Loader" should "dimensionalize a denormalized source" in {

    val source = "PABLO_DWH.T0200_ITAM_INCIDENTS_DH_NEW"
    val df = jdbcReader.read(source)
    val (dimFields, attrs) = conf.dims(source)(4)
    loader.loadDim(df, dimFields, attrs, "test", "test", source)

    val dimCount = sqlContext.sql("select count(*) from itam.dim_priority").first()(0)

    dimCount should be (4)
  }

}

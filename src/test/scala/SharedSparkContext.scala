import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Created by markmo on 23/01/2016.
  */
trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>

  @transient private var _sc: SparkContext = _

  @transient private var _sqlContext: SQLContext = _

  var sparkConf = new SparkConf(false)

  def sc: SparkContext = _sc

  def sqlContext = _sqlContext

  override def beforeAll(): Unit = {
    _sc = new SparkContext("local[*]", "Test", sparkConf)
    _sqlContext = new TestHiveContext(_sc)
  }

  override def afterAll(): Unit = {
    _sqlContext = null
    LocalSparkContext.stop(_sc)
    _sc = null
    super.afterAll()
  }

}

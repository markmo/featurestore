import io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/**
  * Created by markmo on 23/01/2016.
  */
trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>

  @transient var sc: SparkContext = _

  override def beforeAll(): Unit = {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
    super.beforeAll()
  }

  override def afterEach(): Unit = {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext(): Unit = {
    LocalSparkContext.stop(sc)
    sc = null
  }

}

object LocalSparkContext {

  def stop(sc: SparkContext): Unit = {
    if (sc != null) {
      sc.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  /**
    * Runs `f` by passing in `sc` and ensures that `sc` is stopped.
    *
    * @param sc SparkContext
    * @param f  a function that given a SparkContext return type T
    * @tparam T
    * @return T
    */
  def withSpark[T](sc: SparkContext)(f: SparkContext => T): T = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

}
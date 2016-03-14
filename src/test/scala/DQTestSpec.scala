import com.bfm.topnotch.tnengine.TnEngine

/**
  * Created by markmo on 14/03/2016.
  */
class DQTestSpec extends UnitSpec {

  @transient var engine: TnEngine = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://localhost:9000/")
    engine = new TnEngine(sqlContext)
  }

  "The Framework" should "assess data quality of test set 1" in {
    // assess file /example/exampleAssertionInput.parquet
    val path = getClass.getResource("/testsuite/example_plan.json").getPath
    engine.run(path)
  }

  // TODO
  // Find root cause of issue with /base/superstore_sales.csv. Maybe due to
  // "ISO-8859-1" encoding, or null hiveConf reference when using backticks
  // to resolve column name with spaces. (java.lang.NullPointerException at
  // org.apache.hadoop.hive.conf.HiveConf.getVar(HiveConf.java:2605))
  it should "assess data quality of test set 2" in {
    // assess file /base/superstore_sales.parquet
    val path = getClass.getResource("/testsuite/plan.json").getPath
    engine.run(path)
  }
}

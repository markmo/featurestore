import diamond.utility.encodingFunctions._

/**
  * Created by markmo on 19/02/2016.
  */
class ReadEbcdicSpec extends UnitSpec {

  "The framework" should "read and translate an EBCDIC file" in {
    val path = getClass.getResource("testfile").getPath
    val it = localReadEBCDIC(path)
    val lines = it.toList

    lines(2) should startWith ("10SMSDC")
    lines(2) should include ("TELSTRA")
    lines.size should be (116)

    lines.foreach(println)
  }

  it should "read and translate an EBCDIC file on HDFS" in {
    val it = hdfsReadEBCDIC("/base/testfile")
    val lines = it.toList

    lines(2) should startWith ("10SMSDC")
    lines(2) should include ("TELSTRA")
    lines.size should be (116)

    lines.foreach(println)
  }

  // TODO
  // take in a schema and write to a parquet file
}

import common.inference.FileMetadataExtractor._
import common.utility.fileFunctions._

/**
  * Created by markmo on 14/03/2016.
  */
class ExtractFileMetadataSpec extends UnitSpec {

  "FileMetadataExtractor" should "infer metadata from a CSV file" in {
    val path = getClass.getResource(s"base/superstore_sales.csv").getPath
    val sample = readSample(path, maxBytes = 200000, encoding = "ISO-8859-1")
    val (metadata, _) = sniff(sample)

    metadata.columnDelimiter should equal (",")
    metadata.header should be (true)
  }

  it should "infer metadata from a tab delimited file" in {
    val path = getClass.getResource(s"base/superstore_sales.txt").getPath
    val sample = readSample(path, maxBytes = 200000, encoding = "ISO-8859-1")
    val (metadata, _) = sniff(sample)

    metadata.columnDelimiter should equal ("\t")
    metadata.header should be (true)
  }
}

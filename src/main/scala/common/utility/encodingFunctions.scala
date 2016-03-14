package common.utility

import java.io.FileInputStream
import java.net.URI
import java.nio.charset.CodingErrorAction

import diamond.AppConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.Iterator
import scala.io.{Source, Codec}

/**
  * Created by markmo on 27/02/2016.
  */
object encodingFunctions {

  /**
    * Translates an EBCDIC file into readable lines.
    *
    * Extended Binary Coded Decimal Interchange Code (EBCDIC) is an eight-bit
    * character encoding used mainly on IBM mainframe and IBM mid-range
    * computer operating systems. EBCDIC descended from the code used with
    * punched cards.
    *
    * This function will report an error if the input is malformed or a
    * character cannot be mapped.
    *
    * @param path String file path
    * @return Iterator[String] of readable lines
    */
  def localReadEBCDIC(path: String): Iterator[String] = {
    val codec = Codec("ibm500")
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)

    Source.fromInputStream(new FileInputStream(path))(codec).getLines
  }

  /**
    *
    * @param path String file path
    * @param conf AppConfig config object
    * @return Iterator[String] of readable lines
    */
  def hdfsReadEBCDIC(path: String)(implicit conf: AppConfig): Iterator[String] = {
    val fs = FileSystem.get(new URI(conf.data.baseURI), new Configuration())
    val codec = Codec("ibm500")
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)

    Source.fromInputStream(fs.open(new Path(path)))(codec).getLines
  }

}

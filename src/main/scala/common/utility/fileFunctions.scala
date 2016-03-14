package common.utility

import java.io.{File, FileInputStream}
import java.nio.charset.CodingErrorAction

import scala.io.{Codec, Source}

/**
  * Created by markmo on 14/03/2016.
  */
object fileFunctions {

  def readSample(path: String, maxBytes: Int, encoding: String = "UTF-8"): String = {
    val codec = Codec(encoding)
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    val source = Source.fromInputStream(new FileInputStream(new File(path)))(codec)
    source.take(maxBytes).mkString
  }

}

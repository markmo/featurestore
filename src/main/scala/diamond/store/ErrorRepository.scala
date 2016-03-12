package diamond.store

import java.io.OutputStreamWriter
import java.net.URI

import com.github.tototoshi.csv.CSVWriter
import diamond.AppConfig
import diamond.models.TransformationError
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by markmo on 11/01/2016.
  */
class ErrorRepository(implicit val conf: AppConfig) extends Repository {

  import conf.data._

  val path = s"$baseURI/${repository.error.path}"

  val fs = FileSystem.get(new URI(path), new Configuration())

  val filename = repository.error.filename

  def save(errors: List[TransformationError]) = {
    val out = fs.create(new Path(filename), true)
    var writer: CSVWriter = null
    try {
      writer = CSVWriter.open(new OutputStreamWriter(out))
      errors.foreach { error =>
        writer.writeRow(error.toArray)
      }
    } finally {
      if (writer != null) writer.close()
    }
  }

}

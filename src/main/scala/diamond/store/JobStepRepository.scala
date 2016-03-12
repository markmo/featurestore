package diamond.store

import java.io.OutputStreamWriter
import java.net.URI

import com.github.tototoshi.csv.CSVWriter
import diamond.AppConfig
import diamond.models.JobStep
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by markmo on 12/01/2016.
  */
class JobStepRepository(implicit val conf: AppConfig) extends Repository {

  import conf.data._

  val path = s"$baseURI/${repository.jobStep.path}"

  val fs = FileSystem.get(new URI(path), new Configuration())

  val filename = repository.jobStep.filename

  def save(steps: List[JobStep]) = {
    val out = fs.create(new Path(filename), true)
    var writer: CSVWriter = null
    try {
      writer = CSVWriter.open(new OutputStreamWriter(out))
      steps.foreach { step =>
        writer.writeRow(step.toArray)
      }
    } finally {
      if (writer != null) writer.close()
    }
  }

}

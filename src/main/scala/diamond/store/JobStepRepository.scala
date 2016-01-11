package diamond.store

import java.io.OutputStreamWriter
import java.net.URI

import com.github.tototoshi.csv.CSVWriter
import diamond.models.JobStep
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by markmo on 12/01/2016.
  */
class JobStepRepository {

  val BASE_URI = "hdfs://localhost:9000/featurestore/control"

  val fs = FileSystem.get(new URI(BASE_URI), new Configuration())

  val filename = "steps.csv"

  def save(steps: List[JobStep]) = {
    val out = fs.create(new Path(BASE_URI + "/" + filename), true)
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

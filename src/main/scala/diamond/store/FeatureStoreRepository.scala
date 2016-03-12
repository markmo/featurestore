package diamond.store

import java.io.{InputStreamReader, OutputStreamWriter}
import java.net.URI

import com.github.tototoshi.csv._
import diamond.AppConfig
import diamond.models.Feature
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by markmo on 12/12/2015.
  */
class FeatureStoreRepository(implicit val conf: AppConfig) extends Repository {

  import conf.data._

  val path = s"$baseURI/${repository.featureStore.path}"

  val fs = FileSystem.get(new URI(path), new Configuration())

  val dictFilename = repository.featureStore.filename

  def load(): FeatureStore = {
    val store = new FeatureStore
    val in = fs.open(new Path(dictFilename))
    var reader: CSVReader = null
    try {
      reader = CSVReader.open(new InputStreamReader(in))
      store.registeredFeatures ++=
        reader.toStream()
          .map(_.toArray)
          .map(Feature.fromArray)
          .toList
    } finally {
      if (reader != null) reader.close()
    }
    store
  }

  // TODO
  // add versioning
  def save(store: FeatureStore) = {
    val out = fs.create(new Path(dictFilename), true)
    var writer: CSVWriter = null
    try {
      writer = CSVWriter.open(new OutputStreamWriter(out))
      store.registeredFeatures.foreach { feature =>
        writer.writeRow(feature.toArray)
      }
    } finally {
      if (writer != null) writer.close()
    }
  }

}

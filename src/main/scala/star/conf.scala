package star

import java.util.{ArrayList => JList}

import com.typesafe.config.{Config, ConfigValue}
import diamond.Configurable

import scala.collection.JavaConversions._

/**
  * Created by markmo on 12/03/2016.
  */
case class StarConfig(schema: String,
                      baseURI: String,
                      parquetPath: String,
                      parquetSamplePath: String,
                      unknown: String,
                      defaultEndDate: String,
                      sampleSize: Long,
                      env: EnvConfig,
                      dims: Map[String, List[(List[String], List[String])]],
                      facts: Map[String, List[String]],
                      tables: List[String]
                     ) {
  def this(conf: Config) = this(
    conf.getString("schema"),
    conf.getString("base-uri"),
    conf.getString("parquet-path"),
    conf.getString("parquet-sample-path"),
    conf.getString("unknown"),
    conf.getString("default-end-date"),
    conf.getLong("sample-size"),
    EnvConfig(conf.getConfig("env")),
    conf.getObject("dims").map { case (source: String, value: ConfigValue) =>
      (source, value.unwrapped().asInstanceOf[JList[JList[JList[String]]]].map { xs =>
        (xs(0).toList, xs(1).toList)
      }.toList)
    }.toMap,
    conf.getObject("facts").map { case (source: String, value: ConfigValue) =>
      (source, value.unwrapped().asInstanceOf[JList[String]].toList)
    }.toMap,
    conf.getStringList("tables").toList
  )
}

case class EnvConfig(driver: String,
                     host: String,
                     port: Int,
                     user: String,
                     password: String,
                     db: String,
                     instance: String,
                     url: String
                    )

object EnvConfig extends Configurable {
  def apply(conf: Config) = {
    implicit val _conf = conf
    new EnvConfig(
      getAs[String]("driver"),
      getAs[String]("host"),
      getAs[Int]("port"),
      getAs[String]("user"),
      getAs[String]("password"),
      getAs[String]("db"),
      getAs[String]("instance"),
      getAs[String]("url")
    )
  }
}
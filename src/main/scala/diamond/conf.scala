package diamond

import com.typesafe.config.{Config, ConfigObject, ConfigValue}

import scala.collection.JavaConversions._

/**
  * Created by markmo on 13/02/2016.
  */
object conf {

  case class AppConfig(data: DataConfig) {
    def this(conf: Config) = this(
      new DataConfig(conf.getConfig("data"))
    )
  }

  case class DataConfig(baseURI: String, raw: RawSourceConfig, acquisition: AcquisitionConfig) {
    def this(conf: Config) = this(
      conf.getString("base-uri"),
      new RawSourceConfig(conf.getConfig("raw")),
      new AcquisitionConfig(conf.getConfig("acquisition"))
    )
  }

  case class RawSourceConfig(path: String, tables: Map[String, SourceTable]) {
    def this(conf: Config) = this(
      conf.getString("path"),
      conf.getObject("tables").map({
        case (tableName: String, configObject: ConfigObject) => (tableName, SourceTable(configObject.toConfig))
      }).toMap
    )
  }

  case class AcquisitionConfig(path: String, hubs: Map[String, HubTable], satellites: Map[String, SatelliteTable]) {
    def this(conf: Config) = this(
      conf.getString("path"),
      conf.getObject("hubs").map({
        case (tableName: String, configObject: ConfigObject) => (tableName, HubTable(configObject.toConfig))
      }).toMap,
      conf.getObject("satellites").map({
        case (tableName: String, configObject: ConfigObject) => (tableName, SatelliteTable(configObject.toConfig))
      }).toMap
    )
  }

  case class SourceTable(path: String)

  object SourceTable extends Configurable {
    def apply(conf: Config) = {
      implicit val _conf = conf
      new SourceTable(
        getAs[String]("path")
      )
    }
  }

  case class HubTable(isDelta: Boolean,
                      entityType: String,
                      idFields: List[String],
                      idType: String,
                      source: String,
                      tableName: Option[String],
                      validStartTimeField: Option[(String, String)],
                      validEndTimeField: Option[(String, String)],
                      deleteIndicatorField: Option[(String, Any)],
                      newNames: Map[String, String],
                      overwrite: Boolean)

  object HubTable extends Configurable {
    def apply(conf: Config) = {
      implicit val _conf = conf
      new HubTable(
        getAs[Boolean]("delta"),
        getAs[String]("entity-type"),
        getAsList[String]("id-fields"),
        getAs[String]("id-type"),
        getAs[String]("source"),
        getAsOpt[String]("table-name"),
        None,
        None,
        None,
        if (conf.hasPath("new-names")) {
          conf.getObject("new-names").map({
            case (oldName: String, newName: ConfigValue) => (oldName, newName.unwrapped().toString)
          }).toMap
        } else {
          Map()
        },
        getAsOpt[Boolean]("overwrite").getOrElse(false)
      )
    }
  }

  case class SatelliteTable(isDelta: Boolean,
                            tableName: String,
                            idFields: List[String],
                            idType: String,
                            source: String,
                            projection: Option[List[String]],
                            validStartTimeField: Option[(String, String)],
                            validEndTimeField: Option[(String, String)],
                            deleteIndicatorField: Option[(String, Any)],
                            partitionKeys: Option[List[String]],
                            newNames: Map[String, String],
                            overwrite: Boolean,
                            writeChangeTables: Boolean)

  object SatelliteTable extends Configurable {
    def apply(conf: Config) = {
      implicit val _conf = conf
      new SatelliteTable(
        getAs[Boolean]("delta"),
        getAs[String]("table-name"),
        getAsList[String]("id-fields"),
        getAs[String]("id-type"),
        getAs[String]("source"),
        getAsOptList[String]("projection"),
        None,
        None,
        None,
        getAsOptList[String]("partitionKeys"),
        if (conf.hasPath("new-names")) {
          conf.getObject("new-names").map({
            case (oldName: String, newName: ConfigValue) => (oldName, newName.unwrapped().toString)
          }).toMap
        } else {
          Map()
        },
        getAsOpt[Boolean]("overwrite").getOrElse(false),
        getAsOpt[Boolean]("writeChangeTables").getOrElse(false)
      )
    }
  }

  trait Configurable {

    def getAs[T](path: String)(implicit conf: Config): T =
      conf.getAnyRef(path).asInstanceOf[T]

    def getAsOpt[T](path: String)(implicit conf: Config): Option[T] =
      if (conf.hasPath(path)) Some(conf.getAnyRef(path).asInstanceOf[T]) else None

    def getAsList[T](path: String)(implicit conf: Config): List[T] =
      conf.getAnyRefList(path).toList.asInstanceOf[List[T]]

    def getAsOptList[T](path: String)(implicit conf: Config): Option[List[T]] =
      if (conf.hasPath(path)) Some(conf.getAnyRefList(path).toList.asInstanceOf[List[T]]) else None

  }

}

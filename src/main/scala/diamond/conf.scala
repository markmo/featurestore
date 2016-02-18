package diamond

import com.typesafe.config.{Config, ConfigObject, ConfigValue}

import scala.collection.JavaConversions._

/**
  * Created by markmo on 15/02/2016.
  */
case class AppConfig(data: DataConfig) {
  def this(conf: Config) = this(
    new DataConfig(conf.getConfig("data"))
  )
}

case class DataConfig(baseURI: String,
                      meta: MetaConfig,
                      rectype: RectypeConfig,
                      raw: RawSourceConfig,
                      acquisition: AcquisitionConfig) {

  def this(conf: Config) = this(
    conf.getString("base-uri"),
    MetaConfig(conf.getConfig("meta")),
    RectypeConfig(conf.getConfig("rectype")),
    new RawSourceConfig(conf.getConfig("raw")),
    new AcquisitionConfig(conf.getConfig("acquisition"))
  )
}

case class MetaConfig(entityId: String,
                      startTime: String,
                      endTime: String,
                      source: String,
                      processType: String,
                      processId: String,
                      processDate: String,
                      userId: String,
                      hashedValue: String,
                      rectype: String,
                      version: String,
                      openEndDateValue: String,
                      validStartTimeField: String,
                      validEndTimeField: String,
                      validStartTime: String,
                      validEndTime: String,
                      deleteIndicatorField: String)

object MetaConfig extends Configurable {
  def apply(conf: Config) = {
    implicit val _conf = conf
    new MetaConfig(
      getAs[String]("entity-id"),
      getAs[String]("start-time"),
      getAs[String]("end-time"),
      getAs[String]("source"),
      getAs[String]("process-type"),
      getAs[String]("process-id"),
      getAs[String]("process-date"),
      getAs[String]("user-id"),
      getAs[String]("hashed-value"),
      getAs[String]("rectype"),
      getAs[String]("version"),
      getAs[String]("open-end-date-value"),
      getAs[String]("valid-start-time-field"),
      getAs[String]("valid-end-time-field"),
      getAs[String]("valid-start-time"),
      getAs[String]("valid-end-time"),
      getAs[String]("delete-indicator-field")
    )
  }
}

case class RectypeConfig(insert: String, update: String, delete: String)

object RectypeConfig extends Configurable {
  def apply(conf: Config) = {
    implicit val _conf = conf
    new RectypeConfig(
      getAs[String]("insert"),
      getAs[String]("update"),
      getAs[String]("delete")
    )
  }
}

case class RawSourceConfig(path: String, tables: Map[String, SourceTable]) {
  def this(conf: Config) = this(
    conf.getString("path"),
    conf.getObject("tables").map({
      case (tableName: String, configObject: ConfigObject) => (tableName, SourceTable(configObject.toConfig))
    }).toMap
  )
}

case class AcquisitionConfig(path: String,
                             hubs: Map[String, HubTable],
                             satellites: Map[String, SatelliteTable],
                             mappings: Map[String, MappingTable]) {

  def this(conf: Config) = this(
    conf.getString("path"),
    conf.getObject("hubs").map({
      case (tableName: String, configObject: ConfigObject) => (tableName, HubTable(configObject.toConfig))
    }).toMap,
    conf.getObject("satellites").map({
      case (tableName: String, configObject: ConfigObject) => (tableName, SatelliteTable(configObject.toConfig))
    }).toMap,
    conf.getObject("mappings").map({
      case (tableName: String, configObject: ConfigObject) => (tableName, MappingTable(configObject.toConfig))
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
      getAsOpt[Boolean]("write-change-tables").getOrElse(false)
    )
  }
}

case class MappingTable(isDelta: Boolean,
                        entityType: String,
                        idFields1: List[String],
                        idType1: String,
                        idFields2: List[String],
                        idType2: String,
                        confidence: Double,
                        source: String,
                        tableName: Option[String],
                        validStartTimeField: Option[(String, String)],
                        validEndTimeField: Option[(String, String)],
                        deleteIndicatorField: Option[(String, Any)],
                        overwrite: Boolean)

object MappingTable extends Configurable {
  def apply(conf: Config) = {
    implicit val _conf = conf
    new MappingTable(
      getAs[Boolean]("delta"),
      getAs[String]("entity-type"),
      getAsList[String]("src-id-fields"),
      getAs[String]("src-id-type"),
      getAsList[String]("dst-id-fields"),
      getAs[String]("dst-id-type"),
      conf.getDouble("confidence"),
      getAs[String]("source"),
      getAsOpt[String]("table-name"),
      None,
      None,
      None,
      getAsOpt[Boolean]("overwrite").getOrElse(false)
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

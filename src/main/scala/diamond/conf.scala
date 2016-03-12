package diamond

import com.typesafe.config.{Config, ConfigObject, ConfigValue}

import scala.collection.JavaConversions._

/**
  * Created by markmo on 15/02/2016.
  */
case class AppConfig(data: DataConfig,
                     user: Map[String, Any]
                    ) {
  def this(conf: Config) = this(
    new DataConfig(conf.getConfig("data")),
    conf.getObject("user").map({
      case (key: String, value: ConfigValue) => (key, value.unwrapped())
    }).toMap
  )
}

case class DataConfig(baseURI: String,
                      meta: Map[String, String],
                      rectype: Map[String, String],
                      filename: Map[String, String],
                      raw: RawSourceConfig,
                      acquisition: AcquisitionConfig,
                      repository: RepositoryConfig
                     ) {
  def this(conf: Config) = this(
    conf.getString("base-uri"),
    conf.getObject("meta").map({
      case (key: String, value: ConfigValue) => (key, value.unwrapped().toString)
    }).toMap,
    conf.getObject("rectype").map({
      case (key: String, value: ConfigValue) => (key, value.unwrapped().toString)
    }).toMap,
    conf.getObject("filename").map({
      case (key: String, value: ConfigValue) => (key, value.unwrapped().toString)
    }).toMap,
    new RawSourceConfig(conf.getConfig("raw")),
    new AcquisitionConfig(conf.getConfig("acquisition")),
    new RepositoryConfig(conf.getConfig("repository"))
  )
}

case class RepositoryConfig(error: ErrorRepositoryConfig,
                            featureStore: FeatureStoreRepositoryConfig,
                            jobStep: JobStepRepositoryConfig
                           ) {
  def this(conf: Config) = this(
    new ErrorRepositoryConfig(conf.getConfig("error")),
    new FeatureStoreRepositoryConfig(conf.getConfig("feature-store")),
    new JobStepRepositoryConfig(conf.getConfig("job-step"))
  )
}

case class ErrorRepositoryConfig(path: String, filename: String) {
  def this(conf: Config) = this(
    conf.getString("path"),
    conf.getString("filename")
  )
}

case class FeatureStoreRepositoryConfig(path: String, filename: String) {
  def this(conf: Config) = this(
    conf.getString("path"),
    conf.getString("filename")
  )
}

case class JobStepRepositoryConfig(path: String, filename: String) {
  def this(conf: Config) = this(
    conf.getString("path"),
    conf.getString("filename")
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

case class AcquisitionConfig(path: String,
                             hubs: Map[String, HubTable],
                             satellites: Map[String, SatelliteTable],
                             links: Map[String, LinkTable],
                             mappings: Map[String, MappingTable]) {

  def this(conf: Config) = this(
    conf.getString("path"),
    conf.getObject("hubs").map({
      case (tableName: String, configObject: ConfigObject) => (tableName, HubTable(configObject.toConfig))
    }).toMap,
    conf.getObject("satellites").map({
      case (tableName: String, configObject: ConfigObject) => (tableName, SatelliteTable(configObject.toConfig))
    }).toMap,
    conf.getObject("links").map({
      case (tableName: String, configObject: ConfigObject) => (tableName, LinkTable(configObject.toConfig))
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

case class LinkTable(isDelta: Boolean,
                     srcEntityType: String,
                     srcIdFields: List[String],
                     srcIdType: String,
                     dstEntityType: String,
                     dstIdFields: List[String],
                     dstIdType: String,
                     source: String,
                     tableName: Option[String],
                     validStartTimeField: Option[(String, String)],
                     validEndTimeField: Option[(String, String)],
                     deleteIndicatorField: Option[(String, Any)],
                     overwrite: Boolean)

object LinkTable extends Configurable {
  def apply(conf: Config) = {
    implicit val _conf = conf
    new LinkTable(
      getAs[Boolean]("delta"),
      getAs[String]("src-entity-type"),
      getAsList[String]("src-id-fields"),
      getAs[String]("src-id-type"),
      getAs[String]("dst-entity-type"),
      getAsList[String]("dst-id-fields"),
      getAs[String]("dst-id-type"),
      getAs[String]("source"),
      getAsOpt[String]("table-name"),
      None,
      None,
      None,
      getAsOpt[Boolean]("overwrite").getOrElse(false)
    )
  }
}

case class MappingTable(isDelta: Boolean,
                        entityType: String,
                        srcIdFields: List[String],
                        srcIdType: String,
                        dstIdFields: List[String],
                        dstIdType: String,
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

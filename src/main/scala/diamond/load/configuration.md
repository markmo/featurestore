# Configuration

Data loading configuration is done using a configuration file in the resources class path. Configuration uses the [Typesafe config library](https://github.com/typesafehub/config).

Configuration file format can be one of:

* Java properties file
* JSON
* HOCON

A sample format follows:

    acquisition {
      path = factory/acquisition
    
      hubs {
        customer {
          entity-type = customer
          delta = false
          id-fields = [cust_id]
          id-type = id1
          source = ${data.raw.tables.demographics.path}
          new-names {
            cust_id: customer_id
          }
        }
      }
    }

A config file can include other config files, e.g.

    include environment.conf

Configuration is loaded with the following command:

    val conf = new AppConfig(ConfigFactory.load())

A type safe object model under `diamond.conf` wraps the configuration, e.g.

    import conf.data._

    val hubConf = acquisition.hubs("customer")
    import hubConf._

    val demo = sqlContext.read.load(source)

    parquetLoader.loadHub(demo,
      isDelta = isDelta,
      entityType = entityType,
      idFields = idFields,
      idType = idType,
      source = source,
      processType = "Load Full",
      processId = "initial",
      userId = "test",
      newNames = newNames
    )

The configuration options are as follows. All data configuration is under the `data` namespace.

    data {
    
    }

## Base URI

    data {
    
      base-uri = "hdfs://localhost:9000"
      
    }

## Raw Data Source Tables

    data {
    
      raw {
        path = <path to raw data source layer dir>
        tables {
          demographics.path = <e.g. ${data.base-uri}/${data.raw.path}/Customer_Demographics.parquet>
          <... other tables>
        }
      }
    }

### Acquisition Layer Hub Tables

    data {
    
      acquisition {
        path = <path to acquisition layer dir>
        
        hubs {
          <table-name, e.g. customer> {
            entity-type = <type of entity e.g. customer>
            delta = <true|false is the source a delta file or full set>
            id-fields = [<list of id fields>]
            id-type = <type of id e.g. Siebel Number>
            source = <e.g. ${data.raw.tables.demographics.path}>
            table-name = <optional: name of output dir>
            valid-start-time-field = {
              <optional: field name in source to use as valid start time field>: <date-time format>
            }
            valid-end-time-field = {
              <optional: field name in source to use as valid end time field>: <date-time format>
            }
            delete-indicator-field = {
              <optional: field name in source that indicates if the record has been logically deleted>: <the value that indicates a true condition e.g. true|1|Y|etc.>
            }
            new-names {
                <optional: old-name>: <new-name>
                ...
            }
            overwrite = <optional: true|false indicates whether the target file can be recreated, and therefore transaction end times can be updated>
          }
          <... other tables>
        }
      }
    }

### Acquisition Layer Satellite Tables

    data {
    
      acquisition {
        path = <path to acquisition layer dir>
        
        satellites {
          <table-name e.g. customer-demographics> {
            table-name = <name of output dir>
            delta = <true|false is the source a delta file or full set>
            id-fields = [<list of id fields>]
            id-type = <type of id e.g. Siebel Number>
            source = <e.g. ${data.raw.tables.demographics.path}>
            projection = [<the list of fields to select>]
            valid-start-time-field = {
              <optional: field name in source to use as valid start time field>: <date-time format>
            }
            valid-end-time-field = {
              <optional: field name in source to use as valid end time field>: <date-time format>
            }
            delete-indicator-field = {
              <optional: field name in source that indicates if the record has been logically deleted>: <the value that indicates a true condition e.g. true|1|Y|etc.>
            }
            new-names {
                <optional: old-name>: <new-name>
                ...
            }
            overwrite = <optional: true|false indicates whether the target file can be recreated, and therefore transaction end times can be updated>
            write-change-tables: <optional: true|false indicates whether to write change tables ie. additional files with just new records, changed records, and deleted records>
          }
          <... other tables>
        }
      }
    }

### Acquisition Layer Mapping Tables

    data {
    
      acquisition {
        path = <path to acquisition layer dir>
        
        mappings {
          <table-name e.g. email> {
            delta = <true|false is the source a delta file or full set>
            entity-type = <type of entity e.g. customer>
            src-id-fields = [<list of id fields in from-entity>]
            src-id-type = <type of id in from-entity e.g. Siebel Number>
            dst-id-fields = [<list of id fields in to-entity>]
            dst-id-type = <type of id in to-entity e.g. Siebel Number>
            confidence = <double: confidence in the mapping as a probability>
            source = <e.g. ${data.raw.tables.email-mappings.path}>
            table-name = <optional: name of output dir>
            valid-start-time-field = {
              <optional: field name in source to use as valid start time field>: <date-time format>
            }
            valid-end-time-field = {
              <optional: field name in source to use as valid end time field>: <date-time format>
            }
            delete-indicator-field = {
              <optional: field name in source that indicates if the record has been logically deleted>: <the value that indicates a true condition e.g. true|1|Y|etc.>
            }
            overwrite = <optional: true|false indicates whether the target file can be recreated, and therefore transaction end times can be updated>
          }
          <... other tables>
        }
      }
    }

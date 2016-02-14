## Loading a Hub

Loading a Customer Hub can be simplified to calling the following function:

    def registerCustomers(df = <File/Table as DataFrame to load>,
                          isDelta = <true|false is the dataframe a delta or full load>,
                          idField = <the name of the customer id field>,
                          idType = <the type of customer id, e.g. "FNN", "Siebel Customer Number">,
                          source = <the name of the source system or table for logging purposes>,
                          processType = <the name of the process type for logging purposes>,
                          processId = <a unique job id for logging and recovery purposes>,
                          userId = <the user or system account for audit and logging purposes>)

For example:

    val demo = sqlContext.read.load(s"$BASE_URI/$LAYER_RAW/Customer_Demographics.parquet")

    loader.registerCustomers(df = demo,
      isDelta = false,
      idFields = "cust_id",
      idType = "CRM Customer Number",
      source = "test",
      processType = "test",
      processId = "123",
      userId = "d777777"
    )

This function calls another function, which allows more fine-grained control.

    def loadHub(df = <File/Table as DataFrame to load>,
                isDelta = <true|false is the dataframe a delta or full load>,
                entityType = <the type of entity, e.g. customer>,
                idFields = <the list of id fields as the key may be composite>,
                idType = <the type of customer id, e.g. "FNN", "Siebel Customer Number">,
                source = <the name of the source system or table for logging purposes>,
                processType = <the name of the process type for logging purposes>,
                processId = <a unique job id for logging and recovery purposes>,
                userId = <the user or system account for audit and logging purposes>)
                tableName = <the name to use for the hub table. if none is given then the name will be '[entityType]_hub' all in lowercase>
                validStartTimeField = <the field that contains the business valid start time of the record>,
                validEndTimeField = <the field that contains the business valid start time of the record>,
                deleteIndicatorField = <the field that indicates if the record has been logically deleted, and the value that indicates a true condition>,
                newNames = <a map of replacement column names>,
                overwrite = <indicates whether the target file can be recreated, and therefore transaction end times can be updated>)

The load satellite function has a similar signature:

    def loadSatellite(df = <File/Table as DataFrame to load>,
                      isDelta = <true|false is the dataframe a delta or full load>,
                      tableName = <the name to use for the satellite table>
                      idFields = <the list of id fields as the key may be composite>,
                      idType = <the type of customer id, e.g. "FNN", "Siebel Customer Number">,
                      source = <the name of the source system or table for logging purposes>,
                      processType = <the name of the process type for logging purposes>,
                      processId = <a unique job id for logging and recovery purposes>,
                      userId = <the user or system account for audit and logging purposes>)
                      projection = <the list of fields to select>,
                      validStartTimeField = <the field that contains the business valid start time of the record>,
                      validEndTimeField = <the field that contains the business valid start time of the record>,
                      deleteIndicatorField = <the field that indicates if the record has been logically deleted, and the value that indicates a true condition>,
                      partitionKeys = <the list of fields to use as partition keys>,
                      newNames = <a map of replacement column names>,
                      overwrite = <indicates whether the target file can be recreated, and therefore transaction end times can be updated>
                      writeChangeTables = <indicates whether to write change tables>)

This function supports a range of scenarios, for example:

### Scenario A - Load a delta file

Delta files contain only new records since the last extract. Therefore, there is no way to determine deleted records unless there is an indicator field in the data.

However, we can't trust that the file doesn't contain records already processed.

Duplicates are first removed.

Incoming records are joined by entity_id with the latest versions of existing records. Where there is no match, the record is treated as new and therefore an insert (unless the deleteIndicatorField exists and is true).

To determine whether a record is an update, all value fields (excluding metadata fields) are concatenated and hashed. These are compared with the hashed values stored against existing records. Where it fails to match, the record is treated as an update, writing a new record and incrementing the version number.

If 'overwrite' is set to true, the transaction end time is updated on the old record (to the same time as the transaction start time on the new record).

### Scenario B - Load a full set

With a full refresh, deletes can be determined by matching the set of new records against the existing records, and any records in the old set but not in the new set must have been deleted.

## Files created

Raw files are expected to be read from HDFS. The path to raw files is defined by the `LAYER_RAW` variable. For example:

    /base/Customer_Demographics.parquet

Files are "loaded" into the Acquisition Layer, its path defined by the `LAYER_ACQUISITION` variable. The following files are created by default (`tableName = "customer_demographics"`).

    /factory/
        acquisition/
            customer_demographics/
                history.parquet
                current.parquet
                proc.csv
                meta.json

If `overwrite = true` then `prev.parquet` is included, which is the previous version of the entire file before being overwritten.

if `writeChangeTables = true` then change tables are also written.

The full possible set of files for a given satellite load is:

    /factory/
        acquisition/
            customer_demographics/
                history.parquet
                current.parquet
                proc.csv
                meta.json
                prev.parquet
                new.parquet
                changed.parquet
                removed.parquet


<table>
    <tr>
        <th>File
        <th>Description
    </tr>
    <tr>
        <td>history.parquet
        <td>The full history of changes. Multiple records for a given entity may exist, however versioned.
    </tr>
    <tr>
        <td>current.parquet
        <td>A file with only the latest version for each entity.
    </tr>
    <tr>
        <td>proc.csv
        <td>
            <p>Process metadata, including:
            <ul>
                <li>process_id
                <li>process_type
                <li>user_id
                <li>read_count
                <li>duplicates_count
                <li>inserts_count
                <li>updates_count
                <li>deletes_count
                <li>process_time
                <li>process_date
            </ul>
        </td>
    </tr>
    <tr>
        <td>meta.json
        <td>Load parameters, such as names of id fields, id type, valid start and end time fields, etc.
    </tr>
    <tr>
        <td>prev.parquet
        <td>The previous version of the history file before being overwritten.
    </tr>
    <tr>
        <td>new.parquet
        <td>The set of records that have been inserted in the last load. By default, the last 3 days of loads will be retained.
    </tr>
    <tr>
        <td>changed.parquet
        <td>The set of records that have been updated in the last load. By default, the last 3 days of loads will be retained.
    </tr>
    <tr>
        <td>removed.parquet
        <td>The set of records that have been removed in the last load. By default, the last 3 days of loads will be retained.
    </tr>
</table>

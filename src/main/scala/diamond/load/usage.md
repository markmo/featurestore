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

# Feature examples

### Retail

Derive features from a primitive point-of-sale feature type, e.g.:

    (transactionId: String, storeId: String, skuId: String, salesAmount: Double)

* Weekly average maximum spend - for each week over a 12 week period (per HH), calculate total daily sales, take the weekly maximum of those daily sales, then average across the 12 weeks, ignoring weeks with null or negative sales.

* Lapsing HHs - sum the last 6 weeks sales, and the 6 weeks prior to that, subtract the two and if the former is less than 60% of the latter, flag as ‘lapsed’.

* Yearly spend - sum the last 52 weeks sales

* Yearly SKUs - count the number of SKUs purchased in the last 52 weeks

* Yearly distinct SKUs - count the number of distinct SKUs purchased in the last 52 weeks

* Yearly carts - count number of distinct transactions in the last 52 weeks

* Average transaction size over the last 12 months

* Average monthly spend over the last 6 months

* Minimum weekly spend in the last 3 months

* Maximum spend in the last 2 weeks

* Per-SKU (product) features:

    * Yearly purchases - number of times purchased in the last 52 weeks
    
    * Yearly average spend - average amount spent in the last 52 weeks
    
    * Yearly total spend - total amount spent in the last 52 weeks
    
    * Yearly inter-purchase interval (IPI) - number of days between the first and last purchase divided by the number of purchases in the last 52 weeks
    
    * Median monthly spend over the last 2 years
    
    * Maximum spend in the last 6 months

* Purchased category X products at least twice in last 6 weeks

    * the mapping of SKU to the product category may not be known up-front

* Purchased category Y products at least twice in last 12 weeks

### Telco

Derive features from a primitive call-record feature type, e.g.:

    (
      technologyCode:     String,     // e.g. "VC" == voice call, "SM" == SMS
      inbound_outbound:   String,     // e.g. "I" == inbound, "O" == outbound
      callDuration:       Int,        // call duration
      countryCode:        String      // country code
    )

* Total calls in last week

* Total SMS in the last 2 weeks

* Duration of inbound calls in last month

* Difference in total outbound calls in the last month vs 2 months before that

* Proportion of international calls to national calls in last 6 weeks

* Gradient of weekly output call duration over last 2 months - compute total outbound call duration in 7 day windows over the last 7 x 8 = 56 days, then compute the gradient over those 8 values

### Online Media

Derive features from web-log event records, e.g.:

    (
      sessionId: String,
      articleId: string,
      headline: string,
      contentType: string,
      sectionId: String,
      duration: Long
      ...
    )

* Number of times each article was read where the `headline` is not empty and the `content_type` contains the string `story`.

* Total number of events in the last 12 months

* Total number of days in the last 3 month that events occurred

* Average number of daily events between 11am - 3pm over the last 2 months

* Average number of weekend events over the last 3 weeks

* Average number of weekday events over the last 5 weeks

* Mean number of events per session over the last 6 weeks

* Average duration of sessions over the last 1 month

* Gradient of weekly average session duration over the last 8 weeks

* Maximum daily events over the last 1 month

* First quantile (0.1) daily event count over the last 2 months

* Total number of events for section "foo" in the last 6 weeks

* Proportion of events for section "foo" compared to all events in the last 6 weeks

* Gradient of weekly proporition of events for section "foo" compared to all weekly events over the last 8 weeks

* Mean days per month of activity over the last 4 months

### Banking

* TODO


### General time-based

* Number of days since the last call

* Number of weeks since last purchase

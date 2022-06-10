# System Evaluation Report
---

### Developers
* Gabriel PANTIRU
* Stefan PETROVICI
* Vladimir RIPAN
---

### System configuration
#### Network layout
* `3` brokers 
* `1` publisher
* `1` subscriber
#### Publication generation rate per second: `5`
#### Subscriptions count: 10000

---

## Evaluation 

## Scenario #1 : Continous publications flow

* Publications sent: `852`
* Runtime: `3` minutes
* Publication generation rate per seconds: `~5`
* Publications received by the subscriber: `852`
* Subscriber logs: `run_logs\1654843039.00002908.log`
* Publisher logs: `run_logs\1654843055.00005358.log`
* Average latency: `2.10337 ms`
* Latency per event: `run_logs\latency.log`
* Subscription generator config
```
    company_probability=1
    company_equal_frequency=1
    value_probability=1
    drop_probability=1
    variation_probability=0.1
    date_probability=0.1
```

## Scenario #2 : Fixed number of publications

* Publications sent: `10000`
* Subscribtions: 10
* Runtime: `n/a`
* Match rate increase: `x5.36`

### Equal frequency for the `company` field: 100%
* received publications: `1171`
* math rate: `11.71%`
* subscriptions config:
``` 
company_probability=1.0,
company_equal_frequency=1.0,
value_probability=1,
drop_probability=1,
variation_probability=0.1,
date_probability=0.1
```

### Equal frequency for the `company` field: 25%
* received publications: `6277`
* math rate: `62.77%`
* subscriptions config:
```
company_probability=1.0,
company_equal_frequency=0.25,
value_probability=1,
drop_probability=1,
variation_probability=0.1,
date_probability=0.1
```

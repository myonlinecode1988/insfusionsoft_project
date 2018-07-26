# Emitter → Pub/Sub → Message Handler → BigQuery 

## Youtube Demo 
[![IMAGE ALT TEXT HERE](https://img.youtube.com/vi/CeuAMx1qpL0/0.jpg)](https://www.youtube.com/watch?v=CeuAMx1qpL0)

### Prerequisites
* Google Cloud service account with Pub/Sub role
* Service accounts have to authenticated and roles assigned to them via IAM management
* Create a ~/gcp/env_vars.txt file with some basic environment variables:

```
export GOOGLE_APPLICATION_CREDENTIALS=~/gcp/keyfile.json
export SERVICE_ACCOUNT=[your service account email]
export PROJECT_ID=[your project id]
export MY_TOPIC=[your topic name]
export MY_SUBSCRIPTION=[your subscription name]
```

### Introduction
We create an emitter which emits JSON having information like columns A,B,C,D or E alongwith timestamp and table name. The emitter at each timestamp , generates one or more columns with random data. It is then published to a
topic. The message handler listens from the subscriber and  transforms data (adds NULL for non existent columns etc.) and inserts the data as a row in the chosen table. 

### Setup
This step creates a dataset that has 100 tables ("table\_1","table\_2"..."table\_100")
```
./setup/start_making_tables.sh
```
### Emitter
This generates a JSON dataset which is published to the topic 
```
An example JSON
{"table": "table_82", "data": {"B": 24, "E": 4, "C": 9}, "timestamp": "2018-07-26 21:36:16"}
```
We have to run the script each time we want to generate new row of data (see demo)
```
./emitter/start_emitting_on_publisher.sh
```
### Message Handler
This code listens on subscriber and if it finds a data;
transforms it and inserts it as a row in chosen SQL table. You have to run this script once (see demo)
```
./message_handler/start_message_handler.sh
```

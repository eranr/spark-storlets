# spark-storlets
A Spark connector to OpenStack Storlets over OpenStack Swift

Build
-----
At this point the documentation covers the assembly usage:

1. Clone the git repository
2. cd to the repository root directory
3. run sbt/sbt assembly

On a fresh Ubuntu 14.04, one has to first do:

1. sudo add-apt-repository -y ppa:openjdk-r/ppa
2. sudo apt-get update
3. sudo apt-get install -y openjdk-8-jdk
4. apt-get install ca-certificates-java
5. sudo update-ca-certificates -f

Running the tests
-----------------
1. Edit src/test/resources/joss.properties and change the parameters
   according to your setup.
2. run sbt/sbt test from the repository root directory

Configure
---------

Configuration keys without a default value are mandatory.

| key | Description | Default Value |
| --- | ----------- | ------------- |
|swift.auth.username | Swift user | |
|swift.auth.password | Swift user password | |
|swift.auth.tenantname | Swift tenant | |
|swift.auth.url | Keystone url pointing at Storlets enabled Swift cluster | |
|storlets.partitioning.method | Either "partitions" or "chunks" | |
|storlets.partitioning.partitions | For partitions method specifies the number of required partitions | mandaory when method is partitions  |
|storlets.partitioning.chunksize | For chunks method this specifies in MB the partition chunk size | mandatory when method is chunks |
|storlets.storlet.name The storlet name to invoke | |
|storlets.csv.max_record_len | The maximum length in bytes of a record in a line | |
|storlets.csv.max_offset | The maximum csv file offset in bytes that includes the csv header| 1024|
|storlets.csv.delimiter| The csv delimiter | |
|storlets.csv.quote | The csv quote character | |
|storlets.csv.escape | The csv escape character | |

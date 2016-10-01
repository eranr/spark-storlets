# spark-storlets
A Spark connector to OpenStack Storlets over OpenStack Swift

## Build

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

## Running the tests

To run the entire tests suite you will need access to Swift with Stotlets.
The easiest way to do so, is using swift storelts all in one (A.K.A s2aio).
Please refer to http://storlets.readthedocs.io/en/latest/getting_started.html
for the s2aio installation.

Once installed follow the below steps to run the tests:

1. Edit src/test/resources/joss.properties and change the parameters
   according to your setup. If you are using s2aio, then use:

  - user name: tester
  - password: testing
  - account name: test
  - auth url: http://<s2aio host>:5000/v2.0/tokens

2. run sbt/sbt test from the repository root directory

## Configure

### Configuration keys

Configuration keys without a default value are mandatory.
Note that the keys are divided into 3 sets:

1. Swift credentials

2. Partitioning configuration

3. CSV parsing configuration

Currently, the following CSV "configurables" are not yet supported
and appear in the table so that their defaults are known.

- delimiter
- comment
- quote
- escape

| key | Description | Default Value |
| --- | ----------- | ------------- |
|spark.swift.auth.username | Swift user | |
|spark.swift.auth.password | Swift user password | |
|spark.swift.auth.tenantname | Swift tenant | |
|spark.swift.auth.url | Keystone url configured with a Storlets enabled Swift cluster endpoint | |
|spark.storlets.partitioning.method | Either "partitions" or "chunks" | |
|spark.storlets.partitioning.partitions | For partitions method specifies the number of required partitions | mandaory when method is partitions  |
|spark.storlets.partitioning.chunksize | For chunks method specifies the partition chunk size in MB | mandatory when method is chunks |
|spark.storlets.storlet.name | The storlet name to invoke | |
|spark.storlets.csv.max_record_len | The maximum length in bytes of a record in a line | 512 |
|spark.storlets.csv.max_offset | The maximum csv file offset in bytes that includes the csv header| 1024|
|spark.storlets.csv.delimiter| The csv delimiter | ',' |
|spark.storlets.csv.comment| The csv comment character | '#' |
|spark.storlets.csv.quote | The csv quote character | '"' |
|spark.storlets.csv.escape | The csv escape character | '/' |

### Example

The following is a spark-defaults.conf example. 
Note that the mentioned storlet is just a sample one that does not really
do any filtering. Rather it just takes care of records that break between
partitions. A storlet that does CSV filtering at the source is coming soon
to Openstack Storlets.

    spark.swift.auth.username			tester
    spark.swift.auth.password			testing	
    spark.swift.auth.tenantname			test
    spark.swift.auth.url			http://192.168.56.200:5000/v2.0/tokens 	
    spark.storlets.partitioning.method		partitions
    spark.storlets.partitioning.partitions 	3
    #spark.storlets.partitioning.chunksize 	128
    spark.storlets.storlet.name			partitionsidentitystorlet-1.0.jar
    spark.storlets.csv.max_record_len		512
    spark.storlets.csv.max_offset		1024
    #spark.storlets.csv.delimiter		,
    #spark.storlets.csv.comment			#
    #spark.storlets.csv.quote			"
    #spark.storlets.csv.escape			/

## Usage Example

We give below a spark-shell usage example.

Start by editing spark-defaults.conf adding the spark-storlets assembly as a jar:

    spark.jars					/root/spark-storlets/target/spark-storlets-assembly-0.0.1.jar	

The below is from within the Spark shell:

    scala>  val df = sqlContext.load("org.apache.storlets.spark.csv", Map("path" -> "myobjects/meter-1M.csv"))
    df: org.apache.spark.sql.DataFrame = [date: string, index: string, sumHC: string, sumHP: string, vid: string, city: string, state: string, state_abbr: string, lat: string, long: string]

    scala> df.registerTempTable("data")

    scala> val res = sqlContext.sql("select count(vid) from data where (state like 'FRA')")
    res: org.apache.spark.sql.DataFrame = [_c0: bigint]

    scala> res.collectAsList()
    res1: java.util.List[org.apache.spark.sql.Row] = [[1070]]

# The org.apache.storlets.spark.csv Class

The org.apache.storlets.spark.csv is a relation provider with schema.
Belows is a list of configurables the relation accepts in the provided map.
Note that any key provided via the map takes precedence over the global configurations in
spark-defaults.conf

- path. The path is mandatory, and can point either to a specific object or to a container.
  If pointing to the specfic object the path has to be of the form: <container>/<object>.
  If pointing a container that path has to be of the form: <container>

- prefix. Optional. If the path points a container, all objects having the specified prefix will be processed
  If the path points to a single object, the prefix must not be specified.

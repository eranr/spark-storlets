/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storlets.spark.csv;

import java.util.UUID;
import java.util.Properties
import java.io.File
import java.io.FileInputStream
import scala.collection.JavaConversions._
import org.scalatest._
import scala.io.Source

import org.apache.spark.{SparkConf, SparkContext}

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import org.apache.storlets.spark.csv.{StorletCsvContext, StorletCsvConf}
import org.apache.storlets.spark.ConfConstants;

class CsvStorletRddSuite extends FunSuite with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  var props: Properties = new Properties();
  var containerName: String = UUID.randomUUID().toString();
  var account: Account = null
  var container: Container = null
  var sconf: StorletCsvConf = null
  val testFileNames: List[String] = List("records.txt","meter-small-1M.csv","meter-1M.csv")
  var sc: SparkContext = null

  def uploadTestFile(source: String, target: String) {
    val url = getClass.getResource("/")
    val path = url.getFile() + "/" + source;
    val f = new File(path);
    val sobject = container.getObject(target);
    sobject.uploadObject(f);
    sobject.exists();
  }

  override protected def beforeAll() {
    val url = getClass.getResource("/joss.properties")
    props.load(new FileInputStream(url.getFile()))

    val config = new AccountConfig();
    config.setUsername(props.getProperty(ConfConstants.SWIFT_USER));
    config.setPassword(props.getProperty(ConfConstants.SWIFT_PASSWORD));
    config.setAuthUrl(props.getProperty(ConfConstants.SWIFT_AUTH_URL));
    config.setTenantName(props.getProperty(ConfConstants.SWIFT_TENANT));
    config.setMock(false);
    account = new AccountFactory(config).createAccount();


    // Create a temp container
    container = account.getContainer(containerName);
    container.create();
    container.exists();

    // Upload file for tests
    for (name <- testFileNames) {
      uploadTestFile(name, name+"1")
      uploadTestFile(name, name+"2")
    }
  }

  override protected def afterAll() {
    val objects = container.list();
    for (currentObject <- objects)
        currentObject.delete()
    container.delete();
  }

  before {
    val conf = new SparkConf()
      .set(ConfConstants.SWIFT_USER, props.getProperty(ConfConstants.SWIFT_USER))
      .set(ConfConstants.SWIFT_PASSWORD, props.getProperty(ConfConstants.SWIFT_PASSWORD))
      .set(ConfConstants.SWIFT_AUTH_URL, props.getProperty(ConfConstants.SWIFT_AUTH_URL))
      .set(ConfConstants.SWIFT_TENANT, props.getProperty(ConfConstants.SWIFT_TENANT))
      .set(ConfConstants.STORLETS_PARTITIONING_METHOD, ConfConstants.STORLETS_PARTITIONING_METHOD_PARTITIONS)
      .set(ConfConstants.STORLETS_PARTITIONING_PARTITIONS_KEY, "3")
    sconf = new StorletCsvConf(conf, "80", ',', '#', "'".head, '/')
  }

  after {
    sc.stop
  }

  def createConf() : SparkConf = new SparkConf()
      .setAppName("CsvStorletRddSuite")
      .setMaster("local[2]") // 2 threads, some parallelism

  test("Test records complete reading from record.txt") {
    val testFilePath = containerName + "/records.txt1"
    val conf = createConf
    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "50")
         .set(ConfConstants.STORLET_NAME, "partitionsidentitystorlet-1.0.jar")
         .set(ConfConstants.STORLETS_PARTITIONING_METHOD, ConfConstants.STORLETS_PARTITIONING_METHOD_PARTITIONS)
         .set(ConfConstants.STORLETS_PARTITIONING_PARTITIONS_KEY,"1")
    val storletCtx = new StorletCsvContext(sconf, testFilePath, "") 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    assert(rdd.count === 52)
  }

  test("Test records complete reading from meter-small.csv") {
    val testFile = "meter-small-1M.csv1"
    val conf = createConf
    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "80")
         .set(ConfConstants.STORLET_NAME, "partitionsidentitystorlet-1.0.jar")
    val storletCtx = new StorletCsvContext(sconf, containerName + "/" + testFile, "") 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    assert(rdd.count === 9305)
  }

  test("Test records complete reading from meter-small.csv with chunk size") {
    val testFile = "meter-small-1M.csv1"
    val conf = createConf
    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "80")
         .set(ConfConstants.STORLET_NAME, "partitionsidentitystorlet-1.0.jar")
         .set(ConfConstants.STORLETS_PARTITIONING_METHOD, ConfConstants.STORLETS_PARTITIONING_METHOD_CHUNKS)
         .set(ConfConstants.STORLETS_PARTITIONING_CHUNKSIZE_KEY,"1")
    val storletCtx = new StorletCsvContext(sconf, containerName + "/" + testFile, "") 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    assert(rdd.count === 9305)
  }

  test("Test records reading from meter-1MB.csv") {
    val testFile = "meter-1M.csv1"
    val conf = createConf
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "256")
         .set(ConfConstants.STORLET_NAME, "csvstorlet-1.0.jar")
    sc = new SparkContext(conf)
    val storletCtx = new StorletCsvContext(sconf, containerName + "/" + testFile, "")
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "4,6", "EqualTo(6,FRA)")(storletCtx)
    assert(rdd.count === 1070)
  }

  test("Test records complete reading from meter-small.csv via map-partitions") {
    val testFile = "meter-1M.csv1"
    val conf = createConf
    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "256")
         .set(ConfConstants.STORLET_NAME, "csvstorlet-1.0.jar")
    val storletCtx = new StorletCsvContext(sconf, containerName + "/" + testFile, "") 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)

    var counter: Int = 0
    val res = rdd.mapPartitionsWithIndex {
      (index, iter) => {
        val myList = iter.toList
        myList.map(x => x + " -> " + index).iterator
      }
    }
    assert(res.count === 9305)
  }

  test("Test fixed partitions with small size") {
    val testFilePath = containerName + "/records.txt1"
    val conf = createConf
    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "80")
         .set(ConfConstants.STORLET_NAME, "partitionsidentitystorlet-1.0.jar")
         .set(ConfConstants.STORLETS_PARTITIONING_METHOD, ConfConstants.STORLETS_PARTITIONING_METHOD_PARTITIONS)
         .set(ConfConstants.STORLETS_PARTITIONING_PARTITIONS_KEY,"3")
    val storletCtx = new StorletCsvContext(sconf, testFilePath, "") 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    val caught = intercept[java.lang.IllegalArgumentException] {
      val partition = rdd.getPartitions
    }
    assert(caught.getMessage === "Resulting partition size is too small")
  }

  def assert_partition_entry(entry: CsvStorletPartitionEntry,
                             first: Boolean,
                             start: Long,
                             end: Long,
                             containerName: String,
                             objectName: String) {
    assert(entry.firstPartition === first)
    assert(entry.start === start)
    assert(entry.end === end)
    assert(entry.containerName === containerName)
    assert(entry.objectName === objectName)

  }

  test("Test fixed partitions with object split") {
    // Object being broken between partitions (total partition size < object size)
    // We use 6 partitions and two objects
    // meter-1M.csv1, start=58, end=1048558, data size = 1048501, size on disk = 1048559
    // meter-1M.csv2, start=58, end=1048558, data size = 1048501
    // totalSize = 2097002
    // partition size = 349501
    // partition 1. meter-1M.csv1 58, 349557
    // partition 2. meter-1M.csv1 349558, 699057
    // partition 3. meter-1M.csv1 699058, 1048557
    // partition 4. meter-1M.csv2 58, 349557
    // partition 5. meter-1M.csv2 349558, 699057
    // partition 6. meter-1M.csv2 699058, 1048557
    val conf = createConf
    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "256")
         .set(ConfConstants.STORLET_NAME, "csvstorlet-1.0.jar")
         .set(ConfConstants.STORLETS_PARTITIONING_METHOD, ConfConstants.STORLETS_PARTITIONING_METHOD_PARTITIONS)
         .set(ConfConstants.STORLETS_PARTITIONING_PARTITIONS_KEY,"6")
    val storletCtx = new StorletCsvContext(sconf, containerName, "meter-1M.csv") 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    val partitions = rdd.getPartitions
    assert(partitions.length === 6)
    var part = partitions(0).asInstanceOf[CsvStorletPartition]
    assert(part.entries.length === 1)
    var entry = part.entries.head
    assert_partition_entry(entry, true, 58, 349557, containerName, "meter-1M.csv1")
    part = partitions(1).asInstanceOf[CsvStorletPartition]
    assert(part.entries.length === 1)
    entry = part.entries.head
    assert_partition_entry(entry, false, 349558, 699057, containerName, "meter-1M.csv1")
    part = partitions(2).asInstanceOf[CsvStorletPartition]
    assert(part.entries.length === 1)
    entry = part.entries.head
    assert_partition_entry(entry, false, 699058, 1048558, containerName, "meter-1M.csv1")
    part = partitions(3).asInstanceOf[CsvStorletPartition]
    assert(part.entries.length === 1)
    entry = part.entries.head
    assert_partition_entry(entry, true, 58, 349557, containerName, "meter-1M.csv2")
    part = partitions(4).asInstanceOf[CsvStorletPartition]
    assert(part.entries.length === 1)
    entry = part.entries.head
    assert_partition_entry(entry, false, 349558, 699057, containerName, "meter-1M.csv2")
    part = partitions(5).asInstanceOf[CsvStorletPartition]
    assert(part.entries.length === 1)
    entry = part.entries.head
    assert_partition_entry(entry, false, 699058, 1048558, containerName, "meter-1M.csv2")
    
    // full objects in same partition
  }

  test("Test fixed partitions with several objects per partition") {
    // Two objects being packed into one partition (total partition size > object size)
    // We use 1 partition and two objects
    // meter-1M.csv1, start=58, end=1048558, data size = 1048501, size on disk = 1048559
    // meter-1M.csv2, start=58, end=1048558, data size = 1048501
    // totalSize = 2097002
    // partition size = 2097002
    // Entry 1: meter-1M.csv1 58, 1048557, first == true
    // Entry 2: meter-1M.csv2 58, 1048557, first == true
    val conf = createConf
    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "256")
         .set(ConfConstants.STORLET_NAME, "csvstorlet-1.0.jar")
         .set(ConfConstants.STORLETS_PARTITIONING_METHOD, ConfConstants.STORLETS_PARTITIONING_METHOD_PARTITIONS)
         .set(ConfConstants.STORLETS_PARTITIONING_PARTITIONS_KEY,"1")
    val storletCtx = new StorletCsvContext(sconf, containerName, "meter-1M.csv") 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    val partitions = rdd.getPartitions
    assert(partitions.length === 1)
    var part = partitions(0).asInstanceOf[CsvStorletPartition]
    assert(part.entries.length === 2)
    var entry = part.entries(0)
    assert_partition_entry(entry, true, 58, 1048558, containerName, "meter-1M.csv1")
    entry = part.entries(1)
    assert_partition_entry(entry, true, 58, 1048558, containerName, "meter-1M.csv2")

  }

  test("Test chunk size partitions") {
    // Two objects being packed into one partition (total partition size > object size)
    // We use 1 partition and two objects
    // 1MB = 1048576.
    // meter-1M.csv1, start=58, end=1048558, data size = 1048501, size on disk = 1048559
    // meter-1M.csv2, start=58, end=1048558, data size = 1048501
    // A single object data size is just below 1MB, where the residue is < minDataChunk
    // Hence we expect each object to get into its own partition:
    // Partition 1 Entry 1: meter-1M.csv1 58, 1048557, first == true
    // Partition 2 Entry 1: meter-1M.csv2 58, 1048557, first == true
    val conf = createConf
    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "256")
         .set(ConfConstants.STORLET_NAME, "csvstorlet-1.0.jar")
         .set(ConfConstants.STORLETS_PARTITIONING_METHOD, ConfConstants.STORLETS_PARTITIONING_METHOD_CHUNKS)
         .set(ConfConstants.STORLETS_PARTITIONING_CHUNKSIZE_KEY,"1")
    val storletCtx = new StorletCsvContext(sconf, containerName, "meter-1M.csv") 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    val partitions = rdd.getPartitions
    assert(partitions.length === 2)
    var part = partitions(0).asInstanceOf[CsvStorletPartition]
    assert(part.entries.length === 1)
    var entry = part.entries(0)
    assert_partition_entry(entry, true, 58, 1048558, containerName, "meter-1M.csv1")
    part = partitions(1).asInstanceOf[CsvStorletPartition]
    entry = part.entries(0)
    assert_partition_entry(entry, true, 58, 1048558, containerName, "meter-1M.csv2")

  }

  test("Test chunk size partitions with small objects") {
    // Two objects being packed into one partition (total partition size > object size)
    // We use 1 partition and two objects
    // 1MB = 1048576.
    // records.text1, start=12, end=779, data size = 768, size on disk = 780
    // records.text2, start=12, end=779, data size = 768
    // chunk size >> data size in files...
    // Hence we expect the two objects to 'fall' in a single partition:
    // Entry 1: meter-1M.csv1 58, 1048557, first == true
    // Entry 2: meter-1M.csv2 58, 1048557, first == true
    val conf = createConf
    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "20")
         .set(ConfConstants.STORLET_NAME, "csvstorlet-1.0.jar")
         .set(ConfConstants.STORLETS_PARTITIONING_METHOD, ConfConstants.STORLETS_PARTITIONING_METHOD_CHUNKS)
         .set(ConfConstants.STORLETS_PARTITIONING_CHUNKSIZE_KEY,"1")
    val storletCtx = new StorletCsvContext(sconf, containerName, "records.txt") 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    val partitions = rdd.getPartitions
    assert(partitions.length === 1)
    var part = partitions(0).asInstanceOf[CsvStorletPartition]
    assert(part.entries.length === 2)
    var entry = part.entries(0)
    assert_partition_entry(entry, true, 12, 779, containerName, "records.txt1")
    entry = part.entries(1)
    assert_partition_entry(entry, true, 12, 779, containerName, "records.txt2")

  }
}

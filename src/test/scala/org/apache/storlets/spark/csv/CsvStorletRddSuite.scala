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

  def uploadTestFile(name: String) {
    val url = getClass.getResource("/")
    val path = url.getFile() + "/" + name;
    val f = new File(path);
    val sobject = container.getObject(name);
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
    for (name <- testFileNames) uploadTestFile(name)

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

  test("num partitions") {
    val testFilePath = containerName + "/records.txt"
    val conf = new SparkConf()
      .setAppName("CsvStorletRddSuite")
      .setMaster("local[2]") // 2 threads, some parallelism

    sc = new SparkContext(conf)

    sconf.set(ConfConstants.STORLET_NAME,"partitionsidentitystorlet-1.0.jar")
    val storletCtx = new StorletCsvContext(sconf, testFilePath) 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    rdd.numPartitions(100, 12000) shouldBe 120
    rdd.numPartitions(100, 12001) shouldBe 121
    rdd.numPartitions(1024, 2048) shouldBe 2
    rdd.numPartitions(1024, 2047) shouldBe 2
    sc.stop()
  }

  test("Boundaries") {
    val testFilePath = containerName + "/records.txt"
    val conf = new SparkConf()
      .setAppName("CsvStorletRddSuite")
      .setMaster("local[2]") // 2 threads, some parallelism

    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLET_NAME, "partitionsidentitystorlet-1.0.jar")
    val storletCtx = new StorletCsvContext(sconf, testFilePath) 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    var boundaries = rdd.partitionBoundaries(12, 0, 12000)
    for (i <- 0 to 11) {
        boundaries(i) shouldBe (i*1000, (i+1)*1000 - 1)
    }
    boundaries = rdd.partitionBoundaries(12, 0, 12001)
    for (i <- 0 to 10) {
        boundaries(i) shouldBe (i*1000, (i+1)*1000 - 1)
    }
        boundaries(11) shouldBe (11000, 12000)

  }

  def createConf() : SparkConf = new SparkConf()
      .setAppName("CsvStorletRddSuite")
      .setMaster("local[2]") // 2 threads, some parallelism


  test("getPartitions with user defined number") {
    // Additional test for cores...
    val testFilePath = containerName + "/records.txt"
    val conf = createConf
    sc = new SparkContext(conf)

    sconf.set(ConfConstants.STORLET_NAME, "partitionsidentitystorlet-1.0.jar")
    val storletCtx = new StorletCsvContext(sconf, testFilePath) 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    val partitions = rdd.getPartitions
    partitions(0).asInstanceOf[CsvStorletPartition].index shouldBe 0
    partitions(0).asInstanceOf[CsvStorletPartition].start shouldBe 12
    partitions(0).asInstanceOf[CsvStorletPartition].end shouldBe 267
    partitions(1).asInstanceOf[CsvStorletPartition].index shouldBe 1
    partitions(1).asInstanceOf[CsvStorletPartition].start shouldBe 268
    partitions(1).asInstanceOf[CsvStorletPartition].end shouldBe 523
    partitions(2).asInstanceOf[CsvStorletPartition].index shouldBe 2
    partitions(2).asInstanceOf[CsvStorletPartition].start shouldBe 524
    partitions(2).asInstanceOf[CsvStorletPartition].end shouldBe 779
  }

  test("Test records complete reading from record.txt") {
    val testFilePath = containerName + "/records.txt"
    val conf = createConf
    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "80")
         .set(ConfConstants.STORLET_NAME, "partitionsidentitystorlet-1.0.jar")
    val storletCtx = new StorletCsvContext(sconf, testFilePath) 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    assert(rdd.count === 52)
  }

  test("Test records complete reading from meter-small.csv") {
    val testFile = "meter-small-1M.csv"
    val conf = createConf
    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "80")
         .set(ConfConstants.STORLET_NAME, "partitionsidentitystorlet-1.0.jar")
    val storletCtx = new StorletCsvContext(sconf, containerName + "/" + testFile) 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    assert(rdd.count === 9305)
  }

  test("Test records reading from meter-1MB.csv") {
    val testFile = "meter-1M.csv"
    val conf = createConf
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "256")
         .set(ConfConstants.STORLET_NAME, "csvstorlet-1.0.jar")
    sc = new SparkContext(conf)
    val storletCtx = new StorletCsvContext(sconf, containerName + "/" + testFile)
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "4,6", "EqualTo(6,FRA)")(storletCtx)
    assert(rdd.count === 1070)
  }

  test("Test records complete reading from meter-small.csv via map-partitions") {
    val testFile = "meter-1M.csv"
    val conf = createConf
    sc = new SparkContext(conf)
    sconf.set(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, "256")
         .set(ConfConstants.STORLET_NAME, "csvstorlet-1.0.jar")
    val storletCtx = new StorletCsvContext(sconf, containerName + "/" + testFile) 
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

}

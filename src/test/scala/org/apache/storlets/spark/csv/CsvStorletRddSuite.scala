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

import org.apache.storlets.spark.StorletConf;
import org.apache.storlets.spark.csv.StorletCsvContext

class CsvStorletRddSuite extends FunSuite with Matchers with BeforeAndAfter {
  var props: Properties = new Properties();
  var containerName: String = UUID.randomUUID().toString();
  var account: Account = null
  var container: Container = null
  var sconf: StorletConf = null
  var testFilePath: String = null
  val testFileName = "records.txt"
  var sc: SparkContext = null

  def uploadTestFile(name: String) {
    val url = getClass.getResource("/")
    val path = url.getFile() + "/" + name;
    val f = new File(path);
    val sobject = container.getObject(name);
    sobject.uploadObject(f);
    sobject.exists();
  }

  before {
    val url = getClass.getResource("/joss.properties")
    props.load(new FileInputStream(url.getFile()))

    val config = new AccountConfig();
    config.setUsername(props.getProperty("joss.account.user"));
    config.setPassword(props.getProperty("joss.account.password"));
    config.setAuthUrl(props.getProperty("loss.auth.url"));
    config.setTenantName(props.getProperty("joss.account.tenant"));
    config.setMock(false);
    account = new AccountFactory(config).createAccount();

    sconf = new StorletConf()
      .set("swift.storlets.partitions","3")
      .set("storlets.swift.username", props.getProperty("joss.account.user"))
      .set("storlets.swift.password", props.getProperty("joss.account.password"))
      .set("storlets.swift.auth.url", props.getProperty("loss.auth.url"))
      .set("storlets.swift.tenantname", props.getProperty("joss.account.tenant"))
      .set("storlets.csv.max_record_len", "80")

    // Create a temp container
    containerName = UUID.randomUUID().toString();
    container = account.getContainer(containerName);
    container.create();
    container.exists();

    // Upload file for tests
    uploadTestFile(testFileName)

    testFilePath = containerName + "/" + testFileName
  }

  after {
    val objects = container.list();
    for (currentObject <- objects)
        currentObject.delete()
    container.delete();
    sc.stop
  }

  test("num partitions") {
    val conf = new SparkConf()
      .setAppName("CsvStorletRddSuite")
      .setMaster("local[2]") // 2 threads, some parallelism

    sc = new SparkContext(conf)

    val storletCtx = new StorletCsvContext(sconf, testFilePath, ' ', '#', ''', '/') 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    rdd.numPartitions(12, 12000, 1000) shouldBe 12
    rdd.numPartitions(12, 12001, 1000) shouldBe 12
    rdd.numPartitions(12, 12000, 2000) shouldBe 6
    rdd.numPartitions(12, 12001, 2000) shouldBe 7
    sc.stop()
  }

  test("Boundaries") {
    val conf = new SparkConf()
      .setAppName("CsvStorletRddSuite")
      .setMaster("local[2]") // 2 threads, some parallelism

    sc = new SparkContext(conf)
    val storletCtx = new StorletCsvContext(sconf, testFilePath, ' ', '#', ''', '/') 
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
    val conf = createConf

    sc = new SparkContext(conf)

    val storletCtx = new StorletCsvContext(sconf, testFilePath, ' ', '#', ''', '/') 
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

  test("getPartitions with cores") {
    val conf = new SparkConf()
      .setAppName("CsvStorletRddSuite")
      .setMaster("local[2]") // 2 threads, some parallelism
      .set("storlets.swift.replication.factor","3")
      .set("storlets.swift.node.cores","1")
      .set("storlets.minchunk", "100")
      .set("storlets.swift.username", props.getProperty("joss.account.user"))
      .set("storlets.swift.password", props.getProperty("joss.account.password"))
      .set("storlets.swift.auth.url", props.getProperty("loss.auth.url"))
      .set("storlets.swift.tenantname", props.getProperty("joss.account.tenant"))
      .set("storlets.csv.max_record_len","80");
    // Additional test for cores...

    sc = new SparkContext(conf)

    val storletCtx = new StorletCsvContext(sconf, testFilePath, ' ', '#', ''', '/') 
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
    val conf = createConf
    sc = new SparkContext(conf)
    val storletCtx = new StorletCsvContext(sconf, testFilePath, ' ', '#', ''', '/') 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    assert(rdd.count === 52)
  }

  test("Test records complete reading from meter-small.csv") {
    val testFile = "meter-small-1M.csv"
    val conf = createConf
    sc = new SparkContext(conf)
    uploadTestFile(testFile)
    val storletCtx = new StorletCsvContext(sconf, containerName + "/" + testFile , ' ', '#', ''', '/') 
    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)
    assert(rdd.count === 9305)
  }

//  test("Test records complete reading from meter-small.csv via map-partitions") {
//    val conf = createConf
//    sc = new SparkContext(conf)
//    uploadTestFile("meter-small.csv")
//    val storletCtx = new StorletCsvContext(sconf, containerName + "/meter-small.csv" , ' ', '#', ''', '/') 
//    var rdd: CsvStorletRdd = new CsvStorletRdd(sc, sconf, "", "")(storletCtx)

//    var counter: Int = 0
//    rdd.mapPartitions { iter =>
//      iter.flatMap { line  =>
//        counter = counter + 1
//        None
//      }
//      iter
//    }
//    assert(counter === 727227)
//  }
}

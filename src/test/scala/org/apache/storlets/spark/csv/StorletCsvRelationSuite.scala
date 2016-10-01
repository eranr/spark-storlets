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
import org.apache.storlets.spark.ConfConstants;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

class StorletCsvRelationSuite extends FunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfter {
  var props: Properties = new Properties();
  var containerName: String = UUID.randomUUID().toString();
  var account: Account = null
  var container: Container = null
  var testFilePath: String = null
  var sparkConf: SparkConf = null
  val testFileName = "meter-1M.csv"
  var sc: SparkContext = null

  def uploadTestFile(fileName: String, objectName: String) {
    val url = getClass.getResource("/")
    val path = url.getFile() + "/" + fileName;
    val f = new File(path);
    val sobject = container.getObject(objectName);
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
    uploadTestFile(testFileName,testFileName)
    uploadTestFile(testFileName,testFileName+'1')

    testFilePath = containerName + "/" + testFileName

  }

  override protected def afterAll() {
    val objects = container.list();
    for (currentObject <- objects)
        currentObject.delete()
    container.delete();
  }

  before {
    sparkConf = new SparkConf()
      .setAppName("StorletCsvRelationSuite")
      .setMaster("local[2]") // 2 threads, some parallelism
      .set(ConfConstants.STORLETS_PARTITIONING_METHOD, ConfConstants.STORLETS_PARTITIONING_METHOD_PARTITIONS)
      .set(ConfConstants.STORLETS_PARTITIONING_PARTITIONS_KEY, "3")
      .set(ConfConstants.SWIFT_USER, props.getProperty(ConfConstants.SWIFT_USER))
      .set(ConfConstants.SWIFT_PASSWORD, props.getProperty(ConfConstants.SWIFT_PASSWORD))
      .set(ConfConstants.SWIFT_AUTH_URL, props.getProperty(ConfConstants.SWIFT_AUTH_URL))
      .set(ConfConstants.SWIFT_TENANT, props.getProperty(ConfConstants.SWIFT_TENANT))
      .set(ConfConstants.STORLETS_CSV_DELIMITER, " ")
      .set(ConfConstants.STORLETS_CSV_COMMENT, "#")
      .set(ConfConstants.STORLETS_CSV_QUOTE, "'")
      .set(ConfConstants.STORLETS_CSV_ESCAPE, "/");

  }

  after {
    sc.stop
  }

  test("StorletCsvRelation with csvstorlet-1.0.jar and partitions single object") {
    sparkConf.set(ConfConstants.STORLET_NAME, "csvstorlet-1.0.jar")
    sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.load("org.apache.storlets.spark.csv", Map("path" -> testFilePath, "delimiter" -> ","))
    df.registerTempTable("data")
    val res = sqlContext.sql("select count(vid) from data where (state like 'FRA')")
    assert(res.collectAsList()(0)(0) === 1070)
  }

  test("StorletCsvRelation with csvstorlet-1.0.jar and partitions multiple objects") {
    sparkConf.set(ConfConstants.STORLET_NAME, "csvstorlet-1.0.jar")
    sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.load("org.apache.storlets.spark.csv", Map("path" -> containerName, "prefix" -> testFileName))
    df.registerTempTable("data")
    val res = sqlContext.sql("select count(vid) from data where (state like 'FRA')")
    assert(res.collectAsList()(0)(0) === 2140)
  }

  test("StorletCsvRelation with csvstorlet-1.0.jar and partitions multiple objects empty prefix") {
    sparkConf.set(ConfConstants.STORLET_NAME, "csvstorlet-1.0.jar")
    sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.load("org.apache.storlets.spark.csv", Map("path" -> containerName))
    df.registerTempTable("data")
    val res = sqlContext.sql("select count(vid) from data where (state like 'FRA')")
    assert(res.collectAsList()(0)(0) === 2140)
  }

  test("StorletCsvRelation with csvstorlet-1.0.jar and chunks") {
    sparkConf.set(ConfConstants.STORLET_NAME, "csvstorlet-1.0.jar")
      .set(ConfConstants.STORLETS_PARTITIONING_METHOD, ConfConstants.STORLETS_PARTITIONING_METHOD_CHUNKS)
      .set(ConfConstants.STORLETS_PARTITIONING_CHUNKSIZE_KEY, "1")
    sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.load("org.apache.storlets.spark.csv", Map("path" -> testFilePath, "delimiter" -> ","))
    df.registerTempTable("data")
    val res = sqlContext.sql("select count(vid) from data where (state like 'FRA')")
    assert(res.collectAsList()(0)(0) === 1070)
  }
}

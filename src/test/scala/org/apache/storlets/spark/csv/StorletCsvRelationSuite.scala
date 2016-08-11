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

class StorletCsvRelationSuite extends FunSuite with Matchers with BeforeAndAfter {
  var props: Properties = new Properties();
  var containerName: String = UUID.randomUUID().toString();
  var account: Account = null
  var container: Container = null
  var sconf: StorletConf = null
  var testFilePath: String = null
  var sparkConf: SparkConf = null
  //val testFileName = "meter-small-1M.csv"
  val testFileName = "meter-1M.csv"
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

    sparkConf = new SparkConf()
      .setAppName("StorletCsvRelationSuite")
      .setMaster("local[2]") // 2 threads, some parallelism
      .set("swift.storlets.partitions","3")
      .set("storlets.swift.username", props.getProperty("joss.account.user"))
      .set("storlets.swift.password", props.getProperty("joss.account.password"))
      .set("storlets.swift.auth.url", props.getProperty("loss.auth.url"))
      .set("storlets.swift.tenantname", props.getProperty("joss.account.tenant"))

    // Create a temp container
    containerName = UUID.randomUUID().toString();
    container = account.getContainer(containerName);
    container.create();
    container.exists();

    // Upload file for tests
    uploadTestFile(testFileName)

    testFilePath = containerName + "/" + testFileName
    sc = new SparkContext(sparkConf)
  }

  after {
    val objects = container.list();
    for (currentObject <- objects)
        currentObject.delete()
    container.delete();
    sc.stop
  }

  test("StorletCsvRelation") {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.load("org.apache.storlets.spark.csv", Map("path" -> testFilePath, "header" -> "true", "delimiter" -> ","))
    df.registerTempTable("data")
    val res = sqlContext.sql("select count(vid) from data where (state like 'FRA')")
    assert(res.collectAsList()(0)(0) === 1070)
  }
}

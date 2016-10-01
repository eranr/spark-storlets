/*
 * Copyright 2016 itsonlyme
 *
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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.IllegalArgumentException;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Properties;
import java.util.Collection;
import java.util.UUID;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.headers.object.range.FirstPartRange;

import org.apache.spark.SparkConf;
import org.apache.storlets.spark.csv.StorletCsvConf;
import org.apache.storlets.spark.ConfConstants;

public class CsvClientUtilSuite {

  private Account m_account;
  private Properties m_prop;
  private Container m_container;
  private String m_containerName;
  private StorletCsvConf m_sconf;
  private URL m_url;

  @Before
  public void setUp() {
    // Create an account instance
    m_url = this.getClass().getResource("/joss.properties");
    m_prop = new Properties();
    try {
        InputStream input = new FileInputStream(m_url.getFile());
        m_prop.load(input);
    } catch (IOException ex) {
     ex.printStackTrace();
     Assert.assertNotNull(null);   
    }

    SparkConf conf = new SparkConf()
      .set(ConfConstants.SWIFT_USER, m_prop.getProperty(ConfConstants.SWIFT_USER))
      .set(ConfConstants.SWIFT_PASSWORD, m_prop.getProperty(ConfConstants.SWIFT_PASSWORD))
      .set(ConfConstants.SWIFT_AUTH_URL, m_prop.getProperty(ConfConstants.SWIFT_AUTH_URL))
      .set(ConfConstants.SWIFT_TENANT, m_prop.getProperty(ConfConstants.SWIFT_TENANT))
      .set(ConfConstants.STORLET_NAME, "partitionsidentitystorlet-1.0.jar")
      .set(ConfConstants.STORLETS_PARTITIONING_METHOD, ConfConstants.STORLETS_PARTITIONING_METHOD_PARTITIONS)
      .set(ConfConstants.STORLETS_PARTITIONING_PARTITIONS_KEY, "5");

    m_sconf = new StorletCsvConf(conf, "80",',','#', "'".charAt(0), '/');

    AccountConfig config = new AccountConfig();
    config.setUsername(m_prop.getProperty(ConfConstants.SWIFT_USER));
    config.setPassword(m_prop.getProperty(ConfConstants.SWIFT_PASSWORD));
    config.setAuthUrl(m_prop.getProperty(ConfConstants.SWIFT_AUTH_URL));
    config.setTenantName(m_prop.getProperty(ConfConstants.SWIFT_TENANT));
    config.setMock(false);
    m_account = new AccountFactory(config).createAccount();

    // Create a temp container
    m_containerName = UUID.randomUUID().toString();
    m_container = m_account.getContainer(m_containerName);
    m_container.create();
    m_container.exists();
  }

  private void uploadTestFile(String name) {
    m_url = this.getClass().getResource("/");
    String path = m_url.getFile() + "/" + name;
    File f = new File(path);
    StoredObject object = m_container.getObject(name);
    object.uploadObject(f);
    object.exists();
  }

  private InputStream getObjectAsStream(String name) {
    String path = String.format("%s/%s", m_containerName, name);
    Container container = m_account.getContainer(m_containerName);
    StoredObject object = container.getObject(name);
    DownloadInstructions downloadInstructions = new DownloadInstructions().
      setRange(new FirstPartRange(128));
    InputStream is = object.downloadObjectAsInputStream(downloadInstructions);
    return is;
  }

  @After
  public void tearDown() {
    Collection<StoredObject> objects = m_container.list();
    for (StoredObject currentObject : objects) {
        currentObject.delete();
    }
    m_container.delete();
  }

  @Test
  public void testStorletCsvContext() throws StorletCsvException {
    uploadTestFile("cars.csv");  
    String path = String.format("%s/%s", m_containerName, "cars.csv");
    StorletCsvContext ctx = new StorletCsvContext(m_sconf, path, "");
    Assert.assertNotNull(ctx);
    StorletCsvObjectContext object = ctx.getObjects().get(0);
    Assert.assertEquals("object size", 104, object.getSize());
    Assert.assertEquals("first line text", "year,make,model,comment,blank", object.getFirstLine());
    Assert.assertEquals("first line offset", 30, object.getStart());
  }

  @Test
  public void testGetParallelPartitions() {
    int[] partCount = new int[] {3211, 3221 , 2873};

    class consumePartition extends Thread {
      private StorletCsvOutputIterator it;
      private int count;
      private int index;
      public consumePartition(StorletCsvOutputIterator it, int index) {
        this.it = it;
        this.index = index;
        count = 0;
      }

      public void run() {
        while (it.hasNext()) {
          count += 1;
          try {
            it.next();
          } catch (Exception ex) {
            System.out.println("Exception during next. got" + count);
            return;
          }
        }
        Assert.assertEquals(count, partCount[index]);
      }
    }

    String testFileName = "meter-1M.csv";
    uploadTestFile(testFileName);
    String path = String.format("%s/%s", m_containerName, testFileName);
    StoredObject object = m_container.getObject(testFileName);
    StorletCsvOutputIterator it1 = StorletCsvUtils.getCsvStorletOutput(object, "partitionsidentitystorlet-1.0.jar", 0, 58L, 349557L, 80, "", ""); 
    StorletCsvOutputIterator it2 = StorletCsvUtils.getCsvStorletOutput(object, "partitionsidentitystorlet-1.0.jar", 1, 349558L, 699057L, 80, "", ""); 
    StorletCsvOutputIterator it3 = StorletCsvUtils.getCsvStorletOutput(object, "partitionsidentitystorlet-1.0.jar", 2, 699058L, 1048558L, 80, "", "");

    consumePartition p1 = new consumePartition(it1, 0);
    consumePartition p2 = new consumePartition(it2, 1);
    consumePartition p3 = new consumePartition(it3, 2);

    p1.start();
    p2.start();
    p3.start();

    try {
      p1.join();
      p2.join();
      p3.join();
    } catch (Exception ex) {
      System.out.println("Exception during join");
    }
    
  }

  private void uploadTestFileEx(String fileName, String objectName) {
    m_url = this.getClass().getResource("/");
    String path = m_url.getFile() + "/" + fileName;
    File f = new File(path);
    StoredObject object = m_container.getObject(objectName);
    object.uploadObject(f);
    object.exists();
  }

  @Test
  public void test_ctx_container() throws StorletCsvException {
    String testFileName = "meter-1M.csv";
    uploadTestFileEx(testFileName, "object11");
    uploadTestFileEx(testFileName, "object12");
    uploadTestFileEx(testFileName, "object13");
    uploadTestFileEx(testFileName, "object21");
    uploadTestFileEx(testFileName, "object22");
    StorletCsvContext ctx = new StorletCsvContext(m_sconf, m_containerName, "");
    Assert.assertEquals(ctx.getObjects().size(), 5);
  }

  @Test
  public void test_ctx_prefix() throws StorletCsvException {
    String testFileName = "meter-1M.csv";
    uploadTestFileEx(testFileName, "object11");
    uploadTestFileEx(testFileName, "object12");
    uploadTestFileEx(testFileName, "object13");
    uploadTestFileEx(testFileName, "object21");
    uploadTestFileEx(testFileName, "object22");
    StorletCsvContext ctx = new StorletCsvContext(m_sconf, m_containerName, "object1"); 
    Assert.assertEquals(ctx.getObjects().size(), 3);
  }

  @Test(expected=IllegalArgumentException.class)
  public void test_ctx_prefix_object() throws StorletCsvException {
    String testFileName = "meter-1M.csv";
    String path = String.format("%s/%s", m_containerName, testFileName);
    StorletCsvContext ctx = new StorletCsvContext(m_sconf, path, "object1"); 
  }

}

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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

import org.apache.storlets.spark.StorletConf;

public class CsvClientUtilSuite {

  private Account m_account;
  private Properties m_prop;
  private Container m_container;
  private String m_containerName;
  private StorletConf m_sconf;
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

    m_sconf = new StorletConf()
      .set("storlets.swift.username", m_prop.getProperty("joss.account.user"))
      .set("storlets.swift.password", m_prop.getProperty("joss.account.password"))
      .set("storlets.swift.auth.url", m_prop.getProperty("loss.auth.url"))
      .set("storlets.swift.tenantname", m_prop.getProperty("joss.account.tenant"));

    AccountConfig config = new AccountConfig();
    config.setUsername(m_prop.getProperty("joss.account.user"));
    config.setPassword(m_prop.getProperty("joss.account.password"));
    config.setAuthUrl(m_prop.getProperty("loss.auth.url"));
    config.setTenantName(m_prop.getProperty("joss.account.tenant"));
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
  public void testStorletCsvUtilsNameGetters() {
    Account account = null;
    try {
      account = StorletCsvUtils.getAccount(m_sconf);
      Assert.assertNotNull(account);

      String ac = StorletCsvUtils.getContainerName("a/b");
      Assert.assertEquals(ac,"a");
      String ob = StorletCsvUtils.getObjectName("a/b");
      Assert.assertEquals(ob,"b");
    } catch (Exception ex) {
      Assert.assertNotNull(null);
    }
  }

  @Test
  public void testStorletCsvContext() {
    uploadTestFile("cars.csv");  
    String path = String.format("%s/%s", m_containerName, "cars.csv");
    StorletCsvContext ctx = new StorletCsvContext(m_sconf, path, ' ', new Character('#'), new Character('\''), new Character('/'));
    Assert.assertNotNull(ctx);
    Assert.assertEquals("object size", 134, ctx.getObjectSize());
    StorletCsvFirstLine line = ctx.getFirstLine();
    Assert.assertEquals("first line text", "year,make,model,comment,blank", line.getLine());
    Assert.assertEquals("first line offset", 30, line.getOffset());
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
}

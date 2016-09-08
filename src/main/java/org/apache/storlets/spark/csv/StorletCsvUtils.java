/*
 * Copyright 2016 itsonlyme
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storlets.spark.csv;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.headers.GeneralHeader;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.headers.object.range.AbstractRange;

import org.apache.storlets.spark.StorletConf;
import org.apache.storlets.spark.ConfConstants;

public class StorletCsvUtils {

  private final static String OBJ_PATH_FORMAT=" object path '%s' must be of the form container/object";

  private static StorletCsvException malformedPath(String path) {
    return new StorletCsvException(String.format(OBJ_PATH_FORMAT, path));
  }

  public static Account getAccount(StorletConf conf) throws StorletCsvException {
    AccountConfig config = new AccountConfig();
    config.setUsername(conf.get(ConfConstants.SWIFT_USER));
    config.setPassword(conf.get(ConfConstants.SWIFT_PASSWORD));
    config.setAuthUrl(conf.get(ConfConstants.SWIFT_AUTH_URL));
    config.setTenantName(conf.get(ConfConstants.SWIFT_TENANT));
    Account account = new AccountFactory(config).createAccount();
    if (account == null)
      throw malformedPath("Failed to get account");

    return account;
  }

  public static StoredObject getStoredObject(StorletConf conf, String objectName, String containerName) throws StorletCsvException {
    Account account = getAccount(conf);
    Container container = account.getContainer(containerName);
    StoredObject sobject = container.getObject(objectName);
    return sobject;
  }

  public static String getContainerName(String objPath) throws StorletCsvException {
    int i = objPath.indexOf("/");
    if (i <= 0) {
      throw malformedPath(objPath);
    }
    return objPath.substring(0, i);
  }

  public static String getObjectName(String objPath) throws StorletCsvException {
    int i = objPath.indexOf("/");
    if (i <= 0) {
      throw malformedPath(objPath);
    }
    String objName = objPath.substring(i + 1);
    if (objName.isEmpty()) {
      throw malformedPath(objPath);
    }
    return objName;
  }

  public static StorletCsvFirstLine getFirstLine(InputStream is, Character comment) {
    // TODO: Take into account empty / commented lines 
    int bytesRead = 0;
    BufferedReader br = null;
    String line = new String("QQQ");
    try {
      br = new BufferedReader(new InputStreamReader(is));
      line = br.readLine();
      bytesRead += line.length() + 1;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    // The offset of the byte that follows the first line
    // is exactly the number of bytes read as the offset
    // position starts with position 0.
    return new StorletCsvFirstLine(line, bytesRead);
  }

  public static StorletCsvOutputIterator getCsvStorletOutput(StoredObject sobject,
                                                 String storletName,
                                                 int index,
                                                 long start,
                                                 long end,
                                                 int max_record_len,
                                                 String selectedColumns,
                                                 String whereClause) {
    // TODO: add the additional parameters required for CSS parsing
    // e.g. seperator, comment, etc.
    boolean first = (index == 0) ? true : false;
    DownloadInstructions downloadInstructions = new DownloadInstructions()
      .addHeader(new GeneralHeader("X-Run-Storlet", storletName))
      .addHeader(new GeneralHeader("X-Storlet-Range", String.format("bytes=%d-%d",start, end + max_record_len)))
      .addHeader(new GeneralHeader("X-Storlet-Parameter-1", String.format("start:%d", start)))
      .addHeader(new GeneralHeader("X-Storlet-Parameter-2", String.format("end:%d", end)))
      .addHeader(new GeneralHeader("X-Storlet-Parameter-3", String.format("max_record_line:%d", max_record_len)))
      .addHeader(new GeneralHeader("X-Storlet-Parameter-4", String.format("first_partition:%b", first)))
      .addHeader(new GeneralHeader("X-Storlet-Parameter-5", String.format("selected_columns:%s", selectedColumns)))
      .addHeader(new GeneralHeader("X-Storlet-Parameter-6", String.format("where_clause:%s", whereClause)));
    InputStream is = sobject.downloadObjectAsInputStream(downloadInstructions);
    StorletCsvOutputIterator it;
    try {
        it = new StorletCsvOutputIterator(is);
    } catch (Exception ex) {
        it = null;
    }
    return it;
  }
}

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

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.headers.object.range.FirstPartRange;

import org.apache.storlets.spark.StorletConf;

/*
 * This class holds the necessary infomation relevant for
 * running CSV filtering tasks using storlets.
 * This information includes:
 * The object size required to determine the number of partitions
 * The first line of the object required for detemining the schema
 * At this point we support a single object
 *
 * TODO: Support multiple files.
 * Esp. how does databricks support this. Do they have a schema per file? 
 */
public class StorletCsvContext {

  private static final Logger log = LoggerFactory.getLogger(StorletCsvContext.class);

  private Character comment;
  private Account account;
  private Container container;
  private StoredObject sobject;
  private StorletCsvFirstLine firstLine;
  private long objectSize;
  private String containerName;
  private String objectName;
  private int maxOffset;

  /*
   * objPath is assumed to be of the form containerName.objectName
   * commant is a string containing the file's comment symbol
   */
  public StorletCsvContext(StorletConf conf,
                           String objPath) {
    this.maxOffset = Integer.parseInt(conf.get("storlets.csv.header.maxoffset"));
    this.comment = conf.get("storlets.csv.comment").charAt(0);

    try {
        account = StorletCsvUtils.getAccount(conf);
        containerName = StorletCsvUtils.getContainerName(objPath);
        objectName = StorletCsvUtils.getObjectName(objPath);
        container = account.getContainer(containerName);
        sobject = container.getObject(objectName);
    } catch (StorletCsvException ex) {
      log.error("Failed to create StorletCsvContext", ex);
      return;
    }
    DownloadInstructions downloadInstructions = new DownloadInstructions().
      setRange(new FirstPartRange(maxOffset));

    InputStream content = sobject.downloadObjectAsInputStream(downloadInstructions);
    firstLine = StorletCsvUtils.getFirstLine(content, comment);
    objectSize = sobject.getContentLength();
  }

  public long getObjectSize() {
    return objectSize;
  }

  public StorletCsvFirstLine getFirstLine() {
    return firstLine;
  }

  public String getObjectName() {
    return objectName;
  }

  public String getContainerName() {
    return containerName;
  }

}

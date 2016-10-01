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

import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.headers.object.range.FirstPartRange;

import org.apache.storlets.spark.StorletConf;
import org.apache.storlets.spark.ConfConstants;

/*
 * This class holds the necessary infomation relevant for
 * running CSV filtering tasks using storlets.
 * This information includes:
 * The object size required to determine the number of partitions
 * The first line of the object required for detemining the schema
 *
 */
public class StorletCsvContext {

  private static final Logger log = LoggerFactory.getLogger(StorletCsvContext.class);

  private Character comment;
  private Account account;
  private Container container;
  private List<StorletCsvObjectContext> ctxObjects;
  private StorletCsvFirstLine firstLine;
  private String containerName;
  private String objectName;
  private String prefix;
  private int maxOffset;

 /*
  * The object path can reflect
  * A whole container
  * An object
  * A prefix cannot appear together with an object path and vice verca.
  * A whole container can be represented as: string, /string, string/, /string/
  * A container + object can be represented as: <container>/string([/string]*)[/]
  *  where, <container> is any representation of container from above.
  *  ([/string]*) is zero or more /string
  *  [/] zero or more /
  */ 
  private String alignPath(String objPath) {
    // Trim any leading or ending '/'
    objPath = objPath.replaceAll("\\A[/]+","");
    objPath = objPath.replaceAll("[/]+\\z","");

    // Replace doube '/' with single '/'
    objPath = objPath.replaceAll("//+","/");
    return objPath;
  }

  private void parsePath(String objPath) {
    objPath = alignPath(objPath);
    String[] splitPath = objPath.split("/",2);
    containerName = splitPath[0];
    if (splitPath.length == 2) {
      objectName = splitPath[1];
    } else {
      objectName = "";
    }
  }

  public StorletCsvContext(StorletConf conf,
                           String objPath,
                           String prefix) throws IllegalArgumentException, StorletCsvException {
    this.prefix = prefix;
    parsePath(objPath);
    if (prefix != null && !prefix.isEmpty() && !objectName.isEmpty()) {
      throw new IllegalArgumentException("Specifying both a prefix and an object is not allowed");
    }

    this.maxOffset = Integer.parseInt(conf.get(ConfConstants.STORLETS_CSV_MAX_OFFSET));
    this.comment = conf.get(ConfConstants.STORLETS_CSV_COMMENT).charAt(0);

    account = StorletCsvUtils.getAccount(conf);
    container = account.getContainer(containerName);
    ctxObjects = new ArrayList<StorletCsvObjectContext>();
    if (!objectName.isEmpty()) {
      ctxObjects.add(new StorletCsvObjectContext(container.getObject(objectName),
                                                 comment,
                                                 maxOffset));
    } else {
      Collection<StoredObject> objects = container.list(prefix,"",9999);
      for (StoredObject object: objects) {
        ctxObjects.add(new StorletCsvObjectContext(object,
                                                   comment,
                                                   maxOffset));
      }
    }
  }

  public String getFirstLine() {
    return ctxObjects.get(0).getFirstLine();
  }

  public List<StorletCsvObjectContext> getObjects() {
    return ctxObjects;
  }

  public String getContainerName() {
    return containerName;
  }

}

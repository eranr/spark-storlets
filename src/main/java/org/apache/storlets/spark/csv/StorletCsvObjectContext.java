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
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.headers.object.range.FirstPartRange;
import org.javaswift.joss.model.StoredObject;

public class StorletCsvObjectContext {
  private StoredObject sobject;
  private long start;
  private long end;
  private long size;
  private String firstLine;
  private String objectName;
  private String containerName;
  private String path;

  StorletCsvObjectContext(StoredObject sobject, Character comment, int maxOffset) {
    DownloadInstructions downloadInstructions = new DownloadInstructions().
      setRange(new FirstPartRange(maxOffset));

    this.sobject = sobject;
    InputStream content = sobject.downloadObjectAsInputStream(downloadInstructions);
    StorletCsvFirstLine csvFirstLine = StorletCsvUtils.getFirstLine(content, comment);
    firstLine = csvFirstLine.getLine();
    start = csvFirstLine.getOffset();     // The position of the first byte to read
    end = sobject.getContentLength() - 1; // The position of the last byte to read
    size = end - start + 1;               // The total data size in bytes
    path = sobject.getPath();
    String[] pathElements = path.split("/");
    containerName = pathElements[1];
    objectName = pathElements[2];
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public long getSize() {
    return size;
  }

  public String getFirstLine() {
    return firstLine;
  }

  public String getPath() {
    return path;
  }

  public String getObjectName() {
    return objectName;
  }

  public String getContainerName() {
    return containerName;
  }
}

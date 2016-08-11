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

public class StorletCsvFirstLine {
  private String line;
  private int offset;

  public StorletCsvFirstLine(String line, int offset) {
    this.line = line;
    this.offset = offset;
  }

  public String getLine() {
    return line;
  }

  /***
   * offset represetns the offset of the first byte
   * that follows the first line
   */
  public int getOffset() {
    return offset;
  }
}

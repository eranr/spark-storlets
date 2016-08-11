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

import java.util.NoSuchElementException;

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.IOException;

/***
 * Provides a lower level iterator over an Input stream.
 * The upper level needs to take care over the required
 * semantics of the iterator. Also, the upper layer
 * should call close either on an exception or on
 * hasNext = false 
 */
public class StorletCsvOutputIterator {
  private InputStream is; 
  private InputStreamReader isr;
  private BufferedReader br;
  private String prevLine = null;
  private String nextLine = null;
  private boolean has = true;

  public StorletCsvOutputIterator(InputStream is) throws IOException {
    this.is = is;
    this.isr = new InputStreamReader(is);
    br = new BufferedReader(isr);
    prevLine = br.readLine();
    if (prevLine == null)
      has = false;
    nextLine = br.readLine();
  }

  public boolean hasNext() {
    return has;
  }

  public String next() throws IOException {
      if (has == false)
        throw new NoSuchElementException();

      if (nextLine != null) {
        prevLine = nextLine;
        nextLine = br.readLine();
      } else {
        has = false;
      }

      return prevLine;
  }

  public void close() {
    try {
      br.close();
      isr.close();
      is.close();
    } catch (Exception ex) {}
  }

}

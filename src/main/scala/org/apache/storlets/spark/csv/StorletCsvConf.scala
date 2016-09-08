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

import scala.collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.storlets.spark.StorletConf
import org.apache.storlets.spark.ConfConstants

class StorletCsvConf(conf: SparkConf,
                     max_record_len: String,
                     delimiter: Char,
                     quote: Character,
                     escape: Character,
                     comment: Character) extends StorletConf(conf) {
  // The below are expected in every instance of Storlet CSV conf
  settings.put(ConfConstants.STORLETS_CSV_MAX_OFFSET, conf.get(ConfConstants.STORLETS_CSV_MAX_OFFSET, "1024"));
  settings.put(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, conf.get(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN, max_record_len))
  settings.put(ConfConstants.STORLETS_CSV_DELIMITER, conf.get(ConfConstants.STORLETS_CSV_DELIMITER, delimiter.toString))
  settings.put(ConfConstants.STORLETS_CSV_COMMENT, conf.get(ConfConstants.STORLETS_CSV_COMMENT, comment.toString))
  settings.put(ConfConstants.STORLETS_CSV_QUOTE, conf.get(ConfConstants.STORLETS_CSV_QUOTE, quote.toString))
  settings.put(ConfConstants.STORLETS_CSV_ESCAPE, conf.get(ConfConstants.STORLETS_CSV_ESCAPE, escape.toString))
}

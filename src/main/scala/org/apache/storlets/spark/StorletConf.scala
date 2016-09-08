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

/*
 * On Spark 1.6.1 SparkConf is not serialisable
 * so we build our own based on Spark 2.0 SparkConf.
 * At this point we do not need much
 */
package org.apache.storlets.spark;

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import org.apache.spark.SparkConf


class StorletConf(conf: SparkConf) extends Cloneable with Serializable {

  private[storlets] val settings = new ConcurrentHashMap[String, String]()

  // The below are expected in every instance of Storlet conf
  val partitioningMethod: String = conf.getOption(ConfConstants.STORLETS_PARTITIONING_METHOD)
    .getOrElse(throw new NoSuchElementException(ConfConstants.STORLETS_PARTITIONING_METHOD))
  settings.put(ConfConstants.STORLETS_PARTITIONING_METHOD, partitioningMethod)

  if (partitioningMethod == ConfConstants.STORLETS_PARTITIONING_METHOD_PARTITIONS) {
    val partitions: String = conf.getOption(ConfConstants.STORLETS_PARTITIONING_PARTITIONS_KEY)
      .getOrElse(throw new NoSuchElementException(ConfConstants.STORLETS_PARTITIONING_PARTITIONS_KEY))
    settings.put(ConfConstants.STORLETS_PARTITIONING_PARTITIONS_KEY, partitions)
  } else if (partitioningMethod == ConfConstants.STORLETS_PARTITIONING_METHOD_CHUNKS) {
    // chunksize is given in MB
    val chunkSize: String = conf.getOption(ConfConstants.STORLETS_PARTITIONING_CHUNKSIZE_KEY)
      .getOrElse(throw new NoSuchElementException(ConfConstants.STORLETS_PARTITIONING_CHUNKSIZE_KEY))
    settings.put(ConfConstants.STORLETS_PARTITIONING_CHUNKSIZE_KEY, chunkSize)
  } else throw new IllegalArgumentException(partitioningMethod)

  settings.put(ConfConstants.SWIFT_USER, conf.get(ConfConstants.SWIFT_USER))
  settings.put(ConfConstants.SWIFT_PASSWORD, conf.get(ConfConstants.SWIFT_PASSWORD))
  settings.put(ConfConstants.SWIFT_AUTH_URL, conf.get(ConfConstants.SWIFT_AUTH_URL))
  settings.put(ConfConstants.SWIFT_TENANT, conf.get(ConfConstants.SWIFT_TENANT))

  def set(key: String, value: String): StorletConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }

    settings.put(key, value)
    this
  }
  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }
}


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

import scala.collection.JavaConversions._

import org.apache.spark.{InterruptibleIterator, Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import org.apache.storlets.spark.StorletConf
import org.apache.storlets.spark.csv.StorletCsvContext

import org.javaswift.joss.model.StoredObject;

private[storlets] class CsvStorletPartition(idx: Int, val start: Long, val end: Long,
                                            val containerName: String,
                                            val objectName: String,
                                            val storletName: String) extends Partition {
  override def index: Int = idx;
  def firstPartition: Boolean = if (index == 0) true else false;
}

/**
 * :: DeveloperApi ::
 * An RDD that provides core functionality for reading _filtered_ data using a storlet
 * from Openstack Swift empowered with Openstack Storlets
 *
 * @param sc The SparkContext to associate the RDD with.
 * @param sconf
 *    The StorletConf is assumed to hold:
 *    (1) the information needed to connect to Swift.
 *    (2) the swift related configuration for determining
 *    the amount of partitions.
 * @param selectedFields A String representing the fields selection
 * @param whereClause A String representing the row selection 
 * @param transient storletCsvCtx A context holding te infomation
      required to build the specific data source relation.
      This includes the path of the data source as well as the
      CSV parsing information, such as comment, delimiter, etc.
 */
@DeveloperApi
class CsvStorletRdd(
    sc: SparkContext,
    sconf: StorletConf,
    selectedFields: String,
    whereClause: String)(@transient val storletCsvCtx: StorletCsvContext)
  extends RDD[String](sc, Nil) with Logging {

  private val delimiter = storletCsvCtx.getDelimiterChar();
  private val comment = storletCsvCtx.getCommentChar();
  private val quote = storletCsvCtx.getQuoteChar();
  private val escape = storletCsvCtx.getEscapeChar();
  private val maxRecordLen = sconf.get("storlets.csv.max_record_len", "80").toInt

  def numPartitions(totalCores: Int,
                    objectDataSize: Long,
                    minChunkSize: Int) : Int = {
    /* Determine the number of partitions so that we do not
     * stress the object store with too many requests.
     * The below is for a single object. When we support more then
     * one object, this needs to be changed.
     * Ideally the maximum number of requests per object should not exceed
     * the total number of cores we can devote, which is:
     * replication factor * cores in node
     * The user can also specify the minimal chunk size so as to
     * upper bound the number of allocated tasks.
     * If the object size divided by the total number of cores gives
     * a chunk size that is smaller then the min chunk size, then
     * the number of partitions will be determined by the minimal
     * chunk size.
     * The user can ocverride all of the above by setting
     * swift.storlets.partitions to a value greater then zero.
     */
    val partByChunkSize_ = objectDataSize / minChunkSize
    val partByChunkSize = if (objectDataSize % minChunkSize == 0) partByChunkSize_ else partByChunkSize_ + 1

    if (objectDataSize / totalCores < minChunkSize)
      partByChunkSize.toInt
    else
      totalCores
  }

  def partitionBoundaries(numPartitions: Int,
                          firstLineOffset: Int,
                          objectSize: Long): Array[(Long, Long)] = {
    val boundaries = new Array[(Long, Long)](numPartitions)
    val totalData = objectSize - firstLineOffset
    val partitionSize  = totalData / numPartitions
    val residue = totalData % numPartitions
    var start: Long = firstLineOffset.toLong
    for (i  <- 0 to numPartitions - 2) {
      boundaries(i) = (start, start + partitionSize - 1)
      start = start + partitionSize
    }
    boundaries(numPartitions-1) = (start, start + partitionSize -1 + residue)
    boundaries
  }

  override def getPartitions: Array[Partition] = {
    val objectDataSize: Long = storletCsvCtx.getObjectSize() - storletCsvCtx.getFirstLine().getOffset()

    val replicationFactor: Int = sconf.get("storlets.swift.replication.factor", "3").toInt
    val nodeCores: Int = sconf.get("storlets.swift.node.cores", "4").toInt
    val minChunkSize: Int = sconf.get("storlets.minchunk", "134217728").toInt // 128 MB
    val totalCores = replicationFactor * nodeCores
    val userPartitions: Int = sconf.get("swift.storlets.partitions", "0").toInt
    val partitions = if (userPartitions > 0)
      userPartitions
    else
      numPartitions(totalCores, objectDataSize, minChunkSize)
    
    val boundaries = partitionBoundaries(partitions,
                                         storletCsvCtx.getFirstLine().getOffset(),
                                         storletCsvCtx.getObjectSize())
    (0 until boundaries.length).map(i => {
      new CsvStorletPartition(i, boundaries(i)._1, boundaries(i)._2,
                              storletCsvCtx.getContainerName(),
                              storletCsvCtx.getObjectName(),
                              storletCsvCtx.getStorletName())
    }).toArray
  }

  override def compute(thePart: Partition, context: TaskContext): InterruptibleIterator[String] = {
    val thisPart = thePart.asInstanceOf[CsvStorletPartition]
    var sobject: StoredObject = null
    var lower_iter: StorletCsvOutputIterator = null
    var counter: Int = 0

    logInfo("Index: "+thisPart.index+" start " + thisPart.start + " end " + thisPart.end);
    try {
      sobject = StorletCsvUtils.getStoredObject(sconf, thisPart.objectName, thisPart.containerName)
    } catch {
      case e: Exception => {
        logWarning("Exception during getting partition object range", e)
      }
    }

    try {
      lower_iter = StorletCsvUtils.getCsvStorletOutput(sobject,
                                                           thisPart.storletName,
                                                           thisPart.index, thisPart.start, thisPart.end,
                                                           maxRecordLen,
                                                           selectedFields, whereClause)
    } catch {
      case e: Exception => {
        logWarning("Exception during getting partition iterator", e)
      }
    }

    val iter = new Iterator[String]{
      override def hasNext: Boolean = {
        try {
          val has = lower_iter.hasNext()
          if (has == false) {
            logInfo(s"hasNext returning false. So far yielded ${counter} lines")
          }
          has
        } catch {
          case e: Exception => {
            logWarning("Exception during hasNext", e)
            lower_iter.close();
            false
          }
        }
      }

      override def next(): String = {
        var line: String = null
        try {
          line = lower_iter.next()
        } catch {
          case e: Exception => {
            logWarning("Exception during next", e)
            lower_iter.close()
          }
        }

        counter = counter + 1
        line
      }

      def close() {
        logInfo(s"compute iterator closed after yielding ${counter} lines")
        lower_iter.close()
      }
    }
    new InterruptibleIterator(context, iter) 
  }
}

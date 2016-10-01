/*
 * Copyright 2016 itsonlyme
 *
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

import java.util.NoSuchElementException
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.apache.spark.{InterruptibleIterator, Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import org.apache.storlets.spark.StorletConf
import org.apache.storlets.spark.ConfConstants
import org.apache.storlets.spark.csv.{StorletCsvContext, StorletCsvObjectContext}

import org.javaswift.joss.model.StoredObject;

@SerialVersionUID(100L)
private[storlets] class CsvStorletPartitionEntry(val firstPartition: Boolean,
                                                 val start: Long, val end: Long,
                                                 val containerName: String,
                                                 val objectName: String) extends Serializable {
}

/*
 * Ideally we would have passed here a StoredObject instance instead of all its fields...
 * However, it is most likely not serializable.
 */
private[storlets] class CsvStorletPartition(idx: Int, val storletName: String) extends Partition {
  override def index: Int = idx;
  var entries = new ListBuffer[CsvStorletPartitionEntry]()
}

/**
 * :: DeveloperApi ::
 * An RDD that provides core functionality for reading _filtered_ data using a storlet
 * from Openstack Swift empowered with Openstack Storlets
 *
 * @param sc The SparkContext to associate the RDD with.
 * @param sconf
 *    A Storlet CsvConf instance holding
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

  private val delimiter = sconf.get(ConfConstants.STORLETS_CSV_DELIMITER);
  private val comment = sconf.get(ConfConstants.STORLETS_CSV_COMMENT).head;
  private val quote = sconf.get(ConfConstants.STORLETS_CSV_QUOTE).head;
  private val escape = sconf.get(ConfConstants.STORLETS_CSV_ESCAPE).head;
  private val maxRecordLen = sconf.get(ConfConstants.STORLETS_CSV_MAX_RECORD_LEN).toInt
  private val storletName = sconf.get(ConfConstants.STORLET_NAME)
  private val minDataChunk = 10 * maxRecordLen

  def createAndPopulatePartitionsBySize(totalSize: Long,
                                        partitionDataSize: Long,
                                        objectsCtx: java.util.List[StorletCsvObjectContext]): ListBuffer[Partition] = {
    var partitions = new ListBuffer[Partition]() 
    var partitionIdx: Int = 0
    var csvStorletPartition: CsvStorletPartition = new CsvStorletPartition(partitionIdx, storletName)
    partitions.add(csvStorletPartition)
    var partitionSpaceLeft: Long = partitionDataSize
    var totalDataLeft: Long = totalSize
    for (objectCtx <- objectsCtx) {
      var objectDataLeft: Long = objectCtx.getSize()
      var objectPointer: Long = objectCtx.getStart()
      var firstObjectPart: Boolean = true
      //while (objectDataLeft > 0 && partitionIdx < numPartitions) {
      while (objectDataLeft > 0 && totalDataLeft > 0) {
        csvStorletPartition = partitions(partitionIdx).asInstanceOf[CsvStorletPartition]
        var dataFromObject: Long = math.min(objectDataLeft, partitionSpaceLeft)
        // Make sure we do not leave too little to read.
        if (objectDataLeft - dataFromObject < minDataChunk) dataFromObject = objectDataLeft
        val start = objectPointer
        val end = objectPointer + dataFromObject - 1 
        csvStorletPartition.entries.add(new CsvStorletPartitionEntry(firstObjectPart,
                                                                     objectPointer, objectPointer + dataFromObject - 1,
                                                                     objectCtx.getContainerName(), objectCtx.getObjectName))
        objectDataLeft -= dataFromObject
        totalDataLeft -= dataFromObject
        partitionSpaceLeft -= dataFromObject
        objectPointer = objectPointer + dataFromObject
        firstObjectPart = false
        if (partitionSpaceLeft <= minDataChunk && totalDataLeft > 0) {
          partitionIdx += 1
          csvStorletPartition = new CsvStorletPartition(partitionIdx, storletName)
          partitions.add(csvStorletPartition)
          partitionSpaceLeft = partitionDataSize
        }
      }
    }
    partitions
  }

  def getFixedPartitions(totalSize: Long, numPartitions: Int, objectsCtx: java.util.List[StorletCsvObjectContext]): Array[Partition] = {
    /* We think of the partitions as a fixed number buckets to which we start allocate object "splits". The object "splits"
     * are calculated as follows:
     * Denote partitionDataSize as the size each partition needs to deal with. This is basically rounding up the division
     * of partitionDataSize by (numPartitions + 1)
     * We now iterate over all objects, starting to fill the buckets, moving to the next bucket when the previous one is full.
     */
    val partitionDataSize: Long = math.ceil(totalSize / numPartitions).toLong
    if (partitionDataSize < minDataChunk)
      throw new java.lang.IllegalArgumentException("Resulting partition size is too small");
    var partitions = createAndPopulatePartitionsBySize(totalSize, partitionDataSize, objectsCtx)
    partitions.toArray
  }

  def getChunkPartitions(totalSize: Long, chunkSize: Int,  objectsCtx: java.util.List[StorletCsvObjectContext]): Array[Partition] = {
    if (chunkSize < minDataChunk)
      throw new java.lang.IllegalArgumentException("Chunk size is too small");
    val partitionDataSize: Long = chunkSize.toLong
    var partitions = createAndPopulatePartitionsBySize(totalSize, partitionDataSize, objectsCtx)
    partitions.toArray
  }

  override def getPartitions: Array[Partition] = {
    val objectsCtx = storletCsvCtx.getObjects()
    var totalDataSize: Long = 0
    for (objectCtx <- objectsCtx) totalDataSize += objectCtx.getSize()
 
    val partitioningMethod: String = sconf.get(ConfConstants.STORLETS_PARTITIONING_METHOD)
    if (partitioningMethod == ConfConstants.STORLETS_PARTITIONING_METHOD_PARTITIONS) {
      val numPartitions: Int = sconf.get(ConfConstants.STORLETS_PARTITIONING_PARTITIONS_KEY).toInt
      getFixedPartitions(totalDataSize, numPartitions, objectsCtx)
    } else {
      var chunkSize: Int = 1024*1024*sconf.get(ConfConstants.STORLETS_PARTITIONING_CHUNKSIZE_KEY).toInt
      getChunkPartitions(totalDataSize, chunkSize, objectsCtx)
    }


   /* 
    val boundaries = partitionBoundaries(partitions,
                                         objectCtx.getStart(),
                                         objectCtx.getEnd())
    (0 until boundaries.length).map(i => {
      new CsvStorletPartition(i, boundaries(i)._1, boundaries(i)._2,
                              containerName,
                              objectName,
                              storletName)
    }).toArray
    */
  }

  override def compute(thePart: Partition, context: TaskContext): InterruptibleIterator[String] = {
    val thisPart = thePart.asInstanceOf[CsvStorletPartition]
    var counter: Int = 0
    logInfo("Index: " + thisPart.index);

    case class objectIterator(entry: CsvStorletPartitionEntry) extends Iterator[String] {
      logInfo(" start " + entry.start + " end " + entry.end + " container " + entry.containerName + " object " + entry.objectName);
      var object_iter: StorletCsvOutputIterator = null
      var sobject: StoredObject = null
      try {
        sobject = StorletCsvUtils.getStoredObject(sconf, entry.objectName, entry.containerName)
      } catch {
        case e: Exception => {
          logWarning("Exception during getting partition object range", e)
        }
      }

      try {
        object_iter = StorletCsvUtils.getCsvStorletOutput(sobject,
                                                         thisPart.storletName,
                                                         thisPart.index, entry.start, entry.end,
                                                         maxRecordLen,
                                                         selectedFields, whereClause)
      } catch {
        case e: Exception => {
          logWarning("Exception during getting partition iterator", e)
        }
      }

      def hasNext = object_iter.hasNext()

      def next() = {
        var line: String = null
        try {
          line = object_iter.next()
        } catch {
          case e: java.util.NoSuchElementException => {
            object_iter.close()
          }
        }
        counter = counter + 1
        line
      }

      def close() = {
        logInfo(s"compute iterator closed after yielding ${counter} lines")
        object_iter.close()
      }
    }
    val compound_iter = thisPart.entries.foldLeft(Iterator.empty.asInstanceOf[Iterator[String]])(
      (iter: Iterator[String], entry: CsvStorletPartitionEntry) => iter ++ new objectIterator(entry))
    new InterruptibleIterator(context, compound_iter)
  }
}

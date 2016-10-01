package org.apache.storlets.spark.csv;

import org.slf4j.LoggerFactory
import java.text.SimpleDateFormat
import scala.collection.JavaConversions._
import scala.util.control.NonFatal

import org.apache.commons.csv._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import org.apache.storlets.spark.StorletConf
import org.apache.storlets.spark.ConfConstants
import org.apache.storlets.spark.csv.{StorletCsvConf, StorletCsvContext}
import org.apache.storlets.spark.csv.util.{ParseModes, TypeCast, InferSchema}

case class StorletCsvRelation protected[spark] (
    charset: String, 
    location: String,
    prefix: String,
    useHeader: Boolean,
    delimiter: Char,
    quote: Character,
    escape: Character,
    comment: Character,
    parseMode: String,
    ignoreLeadingWhiteSpace: Boolean,
    ignoreTrailingWhiteSpace: Boolean,
    treatEmptyValuesAsNulls: Boolean,
    userSchema: StructType = null,
    inferCsvSchema: Boolean,
    codec: String = null,
    nullValue: String = "",
    dateFormat: String = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan {

  // Share date format object as it is expensive to parse date pattern.
  private val dateFormatter = if (dateFormat != null) new SimpleDateFormat(dateFormat) else null

  private val logger = LoggerFactory.getLogger(StorletCsvRelation.getClass)

  private val failFast = ParseModes.isFailFastMode(parseMode)
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  private val permissive = ParseModes.isPermissiveMode(parseMode)

  private val storletConf = getStorletConf
  @transient private val storletCsvCtx = new StorletCsvContext(storletConf,
                                                               location,
                                                               prefix)

  val defaultCsvFormat =
    CSVFormat.DEFAULT.withRecordSeparator(System.getProperty("line.separator", "\n"))

  override val schema: StructType = inferSchema()

  private def getStorletConf(): StorletConf = {
    val conf = sqlContext.sparkContext.getConf
    val sconf = new StorletCsvConf(conf, "512", delimiter, quote, escape, comment)
    sconf.set(ConfConstants.STORLET_NAME, conf.get(ConfConstants.STORLET_NAME, "partitionsidentitystorlet-1.0.jar"))
  }

  private def tokenRdd(header: Array[String],
                       selectedColumns: String,
                       whereClause: String): RDD[Array[String]] = {
    val csvStorletRdd = new CsvStorletRdd(sqlContext.sparkContext,
                                          storletConf,
                                          selectedColumns,
                                          whereClause)(storletCsvCtx)
    val csvFormat = defaultCsvFormat
        .withDelimiter(delimiter)
        .withQuote(quote)
        .withEscape(escape)
        .withSkipHeaderRecord(false)
        .withHeader(header: _*)
        .withCommentMarker(comment)

    csvStorletRdd.mapPartitions { iter =>
      parseCSV(iter, csvFormat)
    }
  }

  private def getElementID(elementName : String): String = {
    schema.fields.indexWhere(x => x.name equals(elementName)).toString  
  }

  private def requiredColumnstoString(requiredColumns: Array[String]): String = {
   val updatedColumns = requiredColumns.map(x => getElementID(x))
   updatedColumns.mkString(",") 
  }

  private def censorFilters(requiredColumns: Array[String], filters: Array[Filter]) : String = {
    val columnsMap = collection.mutable.Map[String, String]()
    requiredColumns.foreach(x => columnsMap.put(x, getElementID(x)))
    columnsMap.foldLeft(filters.mkString(" "))((t, r) => t.replace(r._1, r._2))
  }

  // The build scan functions were borrowed from
  // package com.databricks.spark.csv.CsvRelation
  override def buildScan: RDD[Row] = {
    logger.info("buildScan")
    val simpleDateFormatter = dateFormatter
    val schemaFields = schema.fields
    val rowArray = new Array[Any](schemaFields.length)
    tokenRdd(schemaFields.map(_.name), "", "").flatMap { tokens =>
      if (dropMalformed && schemaFields.length != tokens.length) {
        logger.warn(s"Dropping malformed line: ${tokens.mkString(",")}")
        None
      } else if (failFast && schemaFields.length != tokens.length) {
        throw new RuntimeException(s"Malformed line in FAILFAST mode: ${tokens.mkString(",")}")
      } else {
        var index: Int = 0
        try {
          index = 0
          while (index < schemaFields.length) {
            val field = schemaFields(index)
            rowArray(index) = TypeCast.castTo(tokens(index), field.dataType, field.nullable,
              treatEmptyValuesAsNulls, nullValue, simpleDateFormatter)
            index = index + 1
          }
          Some(Row.fromSeq(rowArray))
        } catch {
          case aiob: ArrayIndexOutOfBoundsException if permissive =>
            (index until schemaFields.length).foreach(ind => rowArray(ind) = null)
            Some(Row.fromSeq(rowArray))
          case _: java.lang.NumberFormatException |
               _: IllegalArgumentException if dropMalformed =>
            logger.warn("Number format exception. " +
              s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
            None
          case pe: java.text.ParseException if dropMalformed =>
            logger.warn("Parse exception. " +
              s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
            None
        }
      }
    }
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    logger.info("buildPrunedScan")
    buildScan(requiredColumns, Array())
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logger.info("buildPrunedFilteredScan")
    val columnsString = requiredColumnstoString(requiredColumns)
    val filtersString = if (requiredColumns.length == 0) filters.mkString(" ") else censorFilters(requiredColumns, filters)
    val simpleDateFormatter = dateFormatter
    val schemaFields = schema.fields
    val requiredFields = StructType(requiredColumns.map(schema(_))).fields
    val shouldTableScan = (schemaFields.deep == requiredFields.deep) && (filters.length == 0)
    val safeRequiredFields = if (dropMalformed) {
      // If `dropMalformed` is enabled, then it needs to parse all the values
      // so that we can decide which row is malformed.
      requiredFields ++ schemaFields.filterNot(requiredFields.contains(_))
    } else {
      requiredFields
    }
    logger.info(s"Safe required fields=${safeRequiredFields.length.toString}")
    val rowArray = new Array[Any](safeRequiredFields.length)
    if (shouldTableScan) {
      buildScan
    } else {
      tokenRdd(schemaFields.map(_.name),
               columnsString, 
               filtersString).flatMap { tokens =>
        if (dropMalformed && schemaFields.length != tokens.length) {
          logger.warn(s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
          None
        } else if (failFast && schemaFields.length != tokens.length) {
          throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
            s"${tokens.mkString(delimiter.toString)}")
        } else {
          var index: Int = 0
          try {
            while (index < requiredFields.length) {
              val field = requiredFields(index)
              rowArray(index) = TypeCast.castTo(
                tokens(index),
                field.dataType,
                field.nullable,
                treatEmptyValuesAsNulls,
                nullValue,
                simpleDateFormatter
              )
              index = index + 1
            }
            Some(Row.fromSeq(rowArray))
          } catch {
            case _: java.lang.NumberFormatException |
                 _: IllegalArgumentException if dropMalformed =>
              logger.warn("Number format exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
            case pe: java.text.ParseException if dropMalformed =>
              logger.warn("Parse exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
          }
        }
      }
    }
  }

  // The functions below was borrowed from
  // package com.databricks.spark.csv.CsvRelation
  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
      val firstRow = firstLine
      val header = if (useHeader) {
        firstRow
      } else {
        firstRow.zipWithIndex.map { case (value, index) => s"C$index"}
      }
      if (this.inferCsvSchema) {
        val simpleDateFormatter = dateFormatter
        InferSchema(tokenRdd(header, "",""), header, nullValue, simpleDateFormatter)
      } else {
        // By default fields are assumed to be StringType
        val schemaFields = header.map { fieldName =>
          StructField(fieldName.toString, StringType, nullable = true)
        }
        StructType(schemaFields)
      }
    }
  }

  private def parseCSV(iter: Iterator[String],
                       csvFormat: CSVFormat): Iterator[Array[String]] = {
    iter.flatMap { line =>
      try {
        val records = CSVParser.parse(line, csvFormat).getRecords
        if (records.isEmpty) {
          logger.warn(s"Ignoring empty line: $line")
          None
        } else {
          Some(records.head.toArray)
        }
      } catch {
        case NonFatal(e) if !failFast =>
          logger.error(s"Exception while parsing line: $line. ", e)
          None
      }
    }
  }

  /**
   * Returns the first line of the file
   */
  private lazy val firstLine = {
    storletCsvCtx.getFirstLine().split(delimiter.toString())
  }

}

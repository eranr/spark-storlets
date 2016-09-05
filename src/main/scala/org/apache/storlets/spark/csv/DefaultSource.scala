/*
 * Copyright 2014 Databricks
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

import java.nio.charset.Charset

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.storlets.spark.csv.util.{TypeCast}

/**
 * Provides access to CSV data from pure SQL statements (i.e. for users of the
 * JDBC server).
 */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    //parameters.getOrElse("path", sys.error("'path' must be specified for Storlet CSV data."))
    parameters.getOrElse("path", throw new Exception("'path' must be specified for Storlet CSV data."))
  }

  /**
   * Creates a new relation for data store in CSV given parameters.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in CSV given parameters and user supported schema.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): StorletCsvRelation = {

    val path = checkPath(parameters)

   /**
    * We divide the list of parameters below (taken from com.databricks.spark.csv)
    * into
    * (1) Those that serve for raw parsing of CSV. Typically we need to push those
    *     to the storlet which does the parsing
    * (2) Those that serve for post parsing decisions. Some could be pushed
    *     down as they can improve the filtering
    * TODO: pass all the rest parameters to the parsing storlet.
    */

   /**
    * CSV parsing
    * We currently 'push down' only the comment
    */
    val delimiter = TypeCast.toChar(parameters.getOrElse("delimiter", ","))
    val quote = parameters.getOrElse("quote", "\"")
    val quoteChar: Character = if (quote == null) {
      null
    } else if (quote.length == 1) {
      quote.charAt(0)
    } else {
      throw new Exception("Quotation cannot be more than one character.")
    }

    val escape = parameters.getOrElse("escape", "/")
    val escapeChar: Character = if (escape == null) {
      null
    } else if (escape.length == 1) {
      escape.charAt(0)
    } else {
      throw new Exception("Escape character cannot be more than one character.")
    }

    val comment = parameters.getOrElse("comment", "#")
    val commentChar: Character = if (comment == null) {
      null
    } else if (comment.length == 1) {
      comment.charAt(0)
    } else {
      throw new Exception("Comment marker cannot be more than one character.")
    }

    val parseMode = parameters.getOrElse("mode", "PERMISSIVE")

    val useHeader = parameters.getOrElse("header", "false")
    val headerFlag = if (useHeader == "true") {
      true
    } else if (useHeader == "false") {
      false
    } else {
      throw new Exception("Header flag can be true or false")
    }

    val ignoreLeadingWhiteSpace = parameters.getOrElse("ignoreLeadingWhiteSpace", "false")
    val ignoreLeadingWhiteSpaceFlag = if (ignoreLeadingWhiteSpace == "false") {
      false
    } else if (ignoreLeadingWhiteSpace == "true") {
      throw new Exception("Ignore whitesspace is not supported")
    } else {
      throw new Exception("Ignore white space flag can be true or false")
    }
    val ignoreTrailingWhiteSpace = parameters.getOrElse("ignoreTrailingWhiteSpace", "false")
    val ignoreTrailingWhiteSpaceFlag = if (ignoreTrailingWhiteSpace == "false") {
      false
    } else if (ignoreTrailingWhiteSpace == "true") {
      throw new Exception("Ignore whitespace is not supported")
    } else {
      throw new Exception("Ignore white space flag can be true or false")
    }
    val treatEmptyValuesAsNulls = parameters.getOrElse("treatEmptyValuesAsNulls", "false")
    val treatEmptyValuesAsNullsFlag = if (treatEmptyValuesAsNulls == "false") {
      false
    } else if (treatEmptyValuesAsNulls == "true") {
      true
    } else {
      throw new Exception("Treat empty values as null flag can be true or false")
    }

    val charset = parameters.getOrElse("charset", Charset.forName("UTF-8").name())
    // TODO validate charset?

    val inferSchema = parameters.getOrElse("inferSchema", "false")
    val inferSchemaFlag = if (inferSchema == "false") {
      false
    } else if (inferSchema == "true") {
      true
    } else {
      throw new Exception("Infer schema flag can be true or false")
    }
    val nullValue = parameters.getOrElse("nullValue", "")

    val dateFormat = parameters.getOrElse("dateFormat", null)

    val codec = parameters.getOrElse("codec", null)

    StorletCsvRelation(
      charset,
      path,
      headerFlag,
      delimiter,
      quoteChar,
      escapeChar,
      commentChar,
      parseMode,
      ignoreLeadingWhiteSpaceFlag,
      ignoreTrailingWhiteSpaceFlag,
      treatEmptyValuesAsNullsFlag,
      schema,
      inferSchemaFlag,
      codec,
      nullValue,
      dateFormat)(sqlContext)
  }

}

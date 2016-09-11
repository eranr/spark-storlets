package org.apache.storlets.spark;

public class ConfConstants {

  /*
   * Swift authentication information.
   * Used for obtaining access token from Openstack Keystone
   * The user credentials should be of a user that has read access to the
   * data as well as ability to execute storlets.
   * The tenant should reflect a Swift account that is ebabled to run storlets
   */
  public static final String SWIFT_AUTH_PREFIX = "spark.swift.auth";

  public static final String SWIFT_USER = SWIFT_AUTH_PREFIX + ".username";
  public static final String SWIFT_PASSWORD = SWIFT_AUTH_PREFIX + ".password";
  public static final String SWIFT_TENANT = SWIFT_AUTH_PREFIX + ".tenantname";
  public static final String SWIFT_AUTH_URL = SWIFT_AUTH_PREFIX + ".url";

 /*
  * Configuration parameters that are independent from the storlet
  * being used
  */
  public static final String STORLETS_GENERAL_PREFIX = "spark.storlets";

 /*
  * Partitioning methods can be either:
  * partitions - a method where the number of partitions is configured
  * chunks - a method where the chunk size is configured
  */
  public static final String STORLETS_PARTITIONING_METHOD_PARTITIONS = "partitions";
  public static final String STORLETS_PARTITIONING_METHOD_CHUNKS = "chunks";
  public static final String STORLETS_PARTITIONING_PREFIX = STORLETS_GENERAL_PREFIX + ".partitioning";
  public static final String STORLETS_PARTITIONING_METHOD = STORLETS_PARTITIONING_PREFIX + ".method";
  public static final String STORLETS_PARTITIONING_PARTITIONS_KEY = STORLETS_PARTITIONING_PREFIX + ".partitions";
  public static final String STORLETS_PARTITIONING_CHUNKSIZE_KEY = STORLETS_PARTITIONING_PREFIX + ".chunksize";


 /*
  * Storlet to invoke
  */
  public static final String STORLET_NAME = STORLETS_GENERAL_PREFIX + ".storlet.name";

 /*
  * CSV specific configurables
  * The chunk size is specified in MB
  */
 public static final String STORLETS_CSV_PREFIX = STORLETS_GENERAL_PREFIX + ".csv";
 public static final String STORLETS_CSV_MAX_RECORD_LEN = STORLETS_GENERAL_PREFIX + ".max_record_len";
 public static final String STORLETS_CSV_MAX_OFFSET = STORLETS_GENERAL_PREFIX + ".max_offset";
 public static final String STORLETS_CSV_DELIMITER = STORLETS_CSV_PREFIX + ".delimiter";
 public static final String STORLETS_CSV_COMMENT = STORLETS_CSV_PREFIX + ".comment";
 public static final String STORLETS_CSV_QUOTE = STORLETS_CSV_PREFIX + ".quote";
 public static final String STORLETS_CSV_ESCAPE = STORLETS_CSV_PREFIX + ".escape";
}

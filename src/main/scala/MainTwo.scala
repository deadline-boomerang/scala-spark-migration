package com.rohan

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.{Connection, DriverManager, Statement}

object MainTwo {

  def main(args: Array[String]): Unit = {
    // Load configurations from application.conf
    val config = ConfigFactory.load()
    val postgresConfig = config.getConfig("db.postgres")
    val mongoConfig = config.getConfig("db.mongodb")

    val jdbcUrl = postgresConfig.getString("url")
    val user = postgresConfig.getString("user")
    val password = postgresConfig.getString("password")
    val schema = postgresConfig.getString("schema")
    val tables = postgresConfig.getStringList("tables")

    val mongoUri = mongoConfig.getString("uri")
    val mongoDatabase = mongoConfig.getString("database")

    // Initialize Spark session
    val spark = SparkSession.builder
      .appName("PostgresMongoSync")
      .config("spark.mongodb.read.connection.uri", mongoUri)
      .config("spark.mongodb.write.connection.uri", mongoUri)
      .master("local[*]")
      .getOrCreate()
    // Load one record from PostgreSQL
    val boomerangEnrichedContactMappingDF = loadTableFromPostgres(spark, jdbcUrl, schema, tables.get(0), user, password)
    val enrichedContactSocialProfileDF = loadTableFromPostgres(spark, jdbcUrl, schema, tables.get(1), user, password)
    val socialProfileDF = loadTableFromPostgres(spark, jdbcUrl, schema, tables.get(2), user, password)
    val boomerangSocialProfile = loadTableFromPostgres(spark, jdbcUrl, schema, tables.get(3), user, password)
    printDataFrameInfo(boomerangEnrichedContactMappingDF)
    printDataFrameInfo(enrichedContactSocialProfileDF)
    printDataFrameInfo(socialProfileDF)
    printDataFrameInfo(boomerangSocialProfile)
//    val filteredTable2DF = table2DF.filter(table2DF("social_profile_user_name") === "mikemalin").limit(1) // Limiting to 1 record

//    printDataFrameInfo(filteredTable2DF)

    // Define the schema for the location array
    val locationSchema = ArrayType(new StructType()
      .add("city", StringType)
      .add("country", StringType)
      .add("state", StringType)
      .add("default", StringType))

    // Load data from MongoDB
    val mongoDF = loadDataFromMongo(spark, mongoUri, mongoDatabase, "contact-profiles")
      .filter(col("location").isNotNull) // Filter out records where 'location' is null
      .withColumn("location_is_array", when(substring(col("location"), 1, 1) === lit("["), true).otherwise(false)) // Check if location starts with '['

    // Parse 'location' as an array if it's a JSON array string, otherwise leave it null
    val processedDF = mongoDF.withColumn("parsed_location",
      when(col("location_is_array"), from_json(col("location"), locationSchema)) // Parse JSON array if location starts with '['
        .otherwise(null)) // Otherwise, keep it null

    // Filter only rows where the 'parsed_location' is not null
    val validLocationsDF = processedDF.filter(col("parsed_location").isNotNull)

    // Explode the array of location objects
    val explodedDF = validLocationsDF
      .withColumn("location_array", explode(col("parsed_location"))) // Explode the array of location objects
      .withColumn("city", col("location_array.city"))
      .withColumn("state", col("location_array.state"))
      .withColumn("country", col("location_array.country"))
      .withColumn("original_location", col("location_array.default"))

    // Join the filtered table2DF from PostgreSQL with the exploded MongoDB data
//    val joinedDF = filteredTable2DF.join(
//      explodedDF,
//      filteredTable2DF("social_profile_user_name") === explodedDF("userName"),
//      "left"
//    )
//
//    // Select only the required columns for the update
//    val updatedTable2DF = joinedDF
//      .select(
//        filteredTable2DF("social_profile_user_name"),
//        explodedDF("city").alias("city"),
//        explodedDF("country").alias("country"),
//        explodedDF("state").alias("state"),
//        explodedDF("original_location").alias("original_location")
//      )
//
//    // Write the updated data to a temporary table in PostgreSQL
//    updatedTable2DF.write
//      .format("jdbc")
//      .option("url", jdbcUrl)
//      .option("dbtable", s"$schema.temp_table2_updates") // Temporary table
//      .option("user", user)
//      .option("password", password)
//      .mode(SaveMode.Append)
//      .save()
//
//    // Perform SQL update on PostgreSQL to update the original table using a JDBC connection
//    var connection: Connection = null
//    var statement: Statement = null
//
//    try {
//      // Establish the JDBC connection
//      connection = DriverManager.getConnection(jdbcUrl, user, password)
//      statement = connection.createStatement()
//
//      // SQL query to update `table2` from the temporary table based on `social_profile_user_name`
//      val updateSQL = s"""
//        UPDATE $schema.${tables.get(1)} AS t
//        SET
//          city = tmp.city,
//          country = tmp.country,
//          state = tmp.state,
//          original_location = tmp.original_location
//        FROM $schema.temp_table2_updates AS tmp
//        WHERE t.social_profile_user_name = tmp.social_profile_user_name;
//      """
//
//      // Execute the SQL update
////      statement.executeUpdate(updateSQL)
//    } catch {
//      case e: Exception => e.printStackTrace()
//    } finally {
//      if (statement != null) statement.close()
//      if (connection != null) connection.close()
//    }

    // Stop Spark
    spark.stop()
  }

  // Function to load data from PostgreSQL
  def loadTableFromPostgres(spark: SparkSession, jdbcUrl: String, schema: String, table: String, user: String, password: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", s"$schema.$table")
      .option("user", user)
      .option("password", password)
      .load()
  }

  // Function to load data from MongoDB
  def loadDataFromMongo(spark: SparkSession, mongoUri: String, database: String, collection: String): DataFrame = {
    spark.read
      .format("mongodb")
      .option("uri", mongoUri)
      .option("database", database)
      .option("collection", collection)
      .load()
  }

  // Function to print row and column count and column names of a DataFrame
  def printDataFrameInfo(df: DataFrame): Unit = {
    val rowCount = df.count() // Get the number of rows
    val columnCount = df.columns.length // Get the number of columns
    val columnNames = df.columns // Get the column names

    println(s"Row count: $rowCount")
    println(s"Column count: $columnCount")
    println(s"Column names: ${columnNames.mkString(", ")}")
  }
}

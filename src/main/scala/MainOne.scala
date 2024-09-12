package com.rohan

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{col, explode, from_json, lit, substring, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.SaveMode

object MainOne {

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
    val table2DF = loadTableFromPostgres(spark, jdbcUrl, schema, tables.get(1), user, password)
    val filteredTable2DF = table2DF.filter(table2DF("social_profile_user_name") === "mikemalin").limit(1) // Limiting to 1 record

    printDataFrameInfo(filteredTable2DF)

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
    val joinedDF = filteredTable2DF.join(
      explodedDF,
      filteredTable2DF("social_profile_user_name") === explodedDF("userName"),
      "left"
    )

    // Update table2DF columns with the location data from MongoDB
    val updatedTable2DF = joinedDF
      .select(
        filteredTable2DF("id"),
        explodedDF("city").alias("city"),
        explodedDF("country").alias("country"),
        explodedDF("state").alias("state"),
        explodedDF("original_location").alias("original_location")
      )

    // Show the updated record
    updatedTable2DF.show(false) // 'false' to display full column content

    // Now we need to write the updated data back to PostgreSQL
    // Write the DataFrame back to PostgreSQL, updating the relevant columns
    updatedTable2DF.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", s"$schema.${tables.get(1)}") // The table you want to update
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Overwrite) // Overwrite the rows for the matching primary key (id)
      .save()

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

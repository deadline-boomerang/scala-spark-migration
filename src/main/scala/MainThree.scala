package com.rohan

import com.rohan.MainTwo.loadDataFromMongo
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.{Connection, DriverManager, Statement}

object MainThree {

  def main(args: Array[String]): Unit = {
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

    val spark = SparkSession.builder
      .appName("PostgresMongoSync")
      .config("spark.mongodb.read.connection.uri", mongoUri)
      .config("spark.mongodb.write.connection.uri", mongoUri)
      .master("local[*]")
      .getOrCreate()

    val boomerangEnrichedContactMappingDF = loadTableFromPostgres(spark, jdbcUrl, schema, tables.get(0), user, password)
    val enrichedContactSocialProfileDF = loadTableFromPostgres(spark, jdbcUrl, schema, tables.get(1), user, password)
    val socialProfileDF = loadTableFromPostgres(spark, jdbcUrl, schema, tables.get(2), user, password)
    val boomerangSocialProfile = loadTableFromPostgres(spark, jdbcUrl, schema, tables.get(3), user, password)
    printDataFrameInfo(boomerangEnrichedContactMappingDF)
    printDataFrameInfo(enrichedContactSocialProfileDF)
    printDataFrameInfo(socialProfileDF)
    printDataFrameInfo(boomerangSocialProfile)

    val mongoDF = loadDataFromMongo(spark, mongoUri, mongoDatabase, "contact-profiles")
      .filter(col("location").isNotNull) // Filter out records where 'location' is null
      .withColumn("location_is_array", when(substring(col("location"), 1, 1) === lit("["), true).otherwise(false)) // Check if location starts with '['
    printDataFrameInfo(mongoDF)

    val becmDF = prefixColumns(boomerangEnrichedContactMappingDF, "becm_")
    val ecspDF = prefixColumns(enrichedContactSocialProfileDF, "ecsp_")
    val spDF = prefixColumns(socialProfileDF, "sp_")
    val mongoPrefixedDF = prefixColumns(mongoDF, "mongo_")

    // Perform the first join between becmDF and ecspDF
    val enrichedContactJoinDF = becmDF
      .join(ecspDF, becmDF("becm_enriched_contact_id") === ecspDF("ecsp_enriched_contact_id"), "inner")

    // Perform the second join with spDF
    val finalJoinDF = enrichedContactJoinDF
      .join(spDF, ecspDF("ecsp_social_profile_id") === spDF("sp_id"), "inner")

    finalJoinDF.show(10) // Shows first 10 rows of the join result before MongoDB join
    mongoDF.show(10) // Shows first 10 rows from MongoDB collection


    // Now join with mongoPrefixedDF on the 'sp_username' and 'mongo_userName' column
    val finalJoinWithMongoDF = finalJoinDF
      .join(mongoPrefixedDF, lower(finalJoinDF("sp_username")) === lower(mongoPrefixedDF("mongo_userName")), "inner")

    // Get the count of rows after all the joins
    val finalRowCount = finalJoinWithMongoDF.count()

    // Print the row count after joining with MongoDF
    println(s"Row count after joining with MongoDF: $finalRowCount")


    spark.stop()
  }

  def prefixColumns(df: DataFrame, prefix: String): DataFrame = {
    df.columns.foldLeft(df)((df, colName) => df.withColumnRenamed(colName, s"${prefix}${colName}"))
  }

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

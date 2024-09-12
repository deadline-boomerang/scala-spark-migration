package com.rohan

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.ConfigFactory
import com.mongodb.spark._

object Main {

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

    // Load tables from PostgreSQL
    val table1DF = loadTableFromPostgres(spark, jdbcUrl, schema, tables.get(0), user, password)
    val table2DF = loadTableFromPostgres(spark, jdbcUrl, schema, tables.get(1), user, password)

    printDataFrameInfo(table1DF)
    printDataFrameInfo(table2DF)

    // Load data from MongoDB


    val mongoDF = loadDataFromMongo(spark, mongoUri, mongoDatabase, "contact-profiles")
    printDataFrameInfo(mongoDF)



    // Perform matching (example: join on a common column 'id')
//    val matchedDF = table1DF.join(mongoDF, table1DF("id") === mongoDF("id"))
//      .join(table2DF, table1DF("id") === table2DF("id"))

    // Write the matched data back to PostgreSQL (overwrite mode)
//    writeToPostgres(matchedDF, jdbcUrl, schema, "matched_data", user, password)

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

  def loadDataFromMongo(spark: SparkSession, mongoUri: String, database: String, collection: String): DataFrame = {
    spark.read
      .format("mongodb")
      .option("uri", mongoUri)  // Use mongo+srv URI here
      .option("database", database)
      .option("collection", collection)
      .load()
  }



  // Function to write the result back to PostgreSQL
  def writeToPostgres(df: DataFrame, jdbcUrl: String, schema: String, table: String, user: String, password: String): Unit = {
    df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", s"$schema.$table")
      .option("user", user)
      .option("password", password)
      .mode("overwrite")
      .save()
  }

  import org.apache.spark.sql.DataFrame

  // Function to print row and column count and column names of a DataFrame
  def printDataFrameInfo(df: DataFrame): Unit = {
    val rowCount = df.count()           // Get the number of rows
    val columnCount = df.columns.length // Get the number of columns
    val columnNames = df.columns        // Get the column names

    println(s"Row count: $rowCount")
    println(s"Column count: $columnCount")
    println(s"Column names: ${columnNames.mkString(", ")}")
  }
}

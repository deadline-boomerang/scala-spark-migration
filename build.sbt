ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

fork in run := true

javaOptions in run ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "scala-mongo-postgres",
    idePackagePrefix := Some("com.rohan"),
    libraryDependencies ++= Seq(
      // Spark dependencies
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",

      // MongoDB Spark Connector
      // https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.0",

      // PostgreSQL JDBC driver
      "org.postgresql" % "postgresql" % "42.7.3"
      ,

      // Config library for managing settings
      "com.typesafe" % "config" % "1.4.3"
    ),



    //    resolvers += "MongoDB Repository" at "https://repo.mongodb.com/maven2"
    //    , // Add this line

    // Add this to ensure that Spark dependencies can be shaded
    //    mainClass in Compile := Some("com.rohan.Main")
  )


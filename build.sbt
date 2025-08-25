
name := "FraudDetection"

version := "0.1"

scalaVersion := "2.12.18"

resolvers += "MMLSpark Repo" at "https://mmlspark.azureedge.net/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1",
  "org.apache.spark" %% "spark-mllib" % "3.4.1",
  "org.apache.spark" %% "spark-streaming" % "3.4.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.1",
  "io.delta" %% "delta-core" % "2.4.0",
  "org.apache.kafka" % "kafka-clients" % "3.4.1",
  "org.apache.kafka" % "kafka-streams" % "3.4.1",
  "org.apache.kafka" % "kafka-streams-scala_2.12" % "3.4.1",
  "ml.dmlc" %% "xgboost4j-spark" % "1.7.5",
  "ml.dmlc" %% "xgboost4j" % "1.7.5",
  "org.scalameta" %% "munit" % "1.0.0-M7" % Test,
  "com.google.code.gson" % "gson" % "2.10.1",
  "org.postgresql" % "postgresql" % "42.6.0",
  "com.sun.mail" % "javax.mail" % "1.6.2",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "com.esotericsoftware" % "kryo-shaded" % "4.0.2"
)

package project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.PipelineModel

import java.util.Properties
import javax.mail._
import javax.mail.internet._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object streaming extends App {

  val spark = SparkSession.builder()
    .appName("KafkaGBTStreamingWithAlerts")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // ---------------------------
  // Load trained GBT model
  // ---------------------------
  println("Loading trained GBT model from HDFS...")
  val modelPath = "hdfs://localhost:9000/models/gbt_fraud_model1"
  val model = PipelineModel.load(modelPath)
  println("Model loaded successfully.")

  // ---------------------------
  // Read streaming data from Kafka
  // ---------------------------
  println("Reading streaming transactions from Kafka topic 'mlevents'...")
  val kafkaDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "mlevents1")
    .option("startingOffsets", "latest")
    .load()

  // ---------------------------
  // Define schema
  // ---------------------------
  val transactionSchema = StructType(Seq(
    StructField("transactionId", StringType, true),
    StructField("userId", IntegerType, true),
    StructField("amount", DoubleType, true),
    StructField("location", StringType, true),
    StructField("device", StringType, true),
    StructField("merchant", StringType, true),
    StructField("timestamp", TimestampType, true)
  ))

  // ---------------------------
  // Parse JSON
  // ---------------------------
  val transactionsDF = kafkaDF.selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), transactionSchema).as("data"))
    .select("data.*")

  println("Streaming source ready.")

  // ---------------------------
  // Log transform
  // ---------------------------
  val transformedDF = transactionsDF.withColumn("amount", log1p($"amount"))

  // ---------------------------
  // Predictions
  // ---------------------------
  val predictions = model.transform(transformedDF)

  // ---------------------------
  // Add fraud_reason
  // ---------------------------
  val explainedDF = predictions.withColumn("fraud_reason",
    when($"prediction" === 1.0,
      concat_ws(",",
        when($"amount" > 7000, "HighAmount"),
        when($"device".isin("Mobile", "ATM"), "RiskyDevice"),
        when($"merchant".isin("Electronics", "Travel"), "RiskyMerchant"),
        when($"location".isin("TX", "FL"), "RiskyLocation")
      )
    ).otherwise(lit("Normal"))
  ).withColumn("fraud_reason",
    when($"prediction" === 1.0 && length($"fraud_reason") === 0, "ModelDetected")
      .otherwise($"fraud_reason")
  )

  // ---------------------------
  // Final output selection
  // ---------------------------
  val output = explainedDF.select(
    "transactionId",
    "userId",
    "amount",
    "location",
    "device",
    "merchant",
    "prediction",
    "fraud_reason"
  )

  // ---------------------------
  // PostgreSQL config
  // ---------------------------
  val pgURL = "jdbc:postgresql://localhost:5432/fraud_analysis1"
  val pgProperties = new Properties()
  pgProperties.setProperty("user", "postgres")
  pgProperties.setProperty("password", "postgre")
  pgProperties.setProperty("driver", "org.postgresql.Driver")

  // ---------------------------
  // Email alert function
  // ---------------------------
  def sendEmailAlertAsync(txnId: String, userId: Int, amount: Double, reason: String): Unit = {
    Future {
      val props = new Properties()
      props.put("mail.smtp.host", "smtp.gmail.com")
      props.put("mail.smtp.port", "587")
      props.put("mail.smtp.auth", "true")
      props.put("mail.smtp.starttls.enable", "true")

      val session = Session.getInstance(props, new Authenticator() {
        override def getPasswordAuthentication =
          new PasswordAuthentication("sallasahas15@gmail.com", "mlec iyef cggc msls")
      })

      val msg = new MimeMessage(session)
      msg.setFrom(new InternetAddress("sallasahas15@gmail.com"))
      msg.setRecipients(Message.RecipientType.TO, "remya.gopalakrishnan@revature.com")
      msg.setSubject("Fraud Alert!")
      msg.setText(s"Fraud detected! Transaction: $txnId, User: $userId, Amount: $amount, Reason: $reason")

      Transport.send(msg)
      println(s"Email alert sent for transaction $txnId")
    }
  }

  // ---------------------------
  // Streaming to PostgreSQL and sending alerts
  // ---------------------------
  val pgQuery = output.writeStream
    .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
      println(s"Processing batch $batchId with ${batchDF.count()} rows")

      // Save to PostgreSQL
      batchDF.write.mode("append").jdbc(pgURL, "transaction_predictions", pgProperties)

      // Send email alerts for fraud transactions
      val fraudDF = batchDF.filter($"prediction" === 1)
      fraudDF.collect().foreach { row =>
        val txnId = row.getAs[String]("transactionId")
        val userId = row.getAs[Int]("userId")
        val amount = row.getAs[Double]("amount")
        val reason = row.getAs[String]("fraud_reason")
        sendEmailAlertAsync(txnId, userId, amount, reason)
      }
    }
    .outputMode("append")
    .start()

  // ---------------------------
  // Console output for debugging
  // ---------------------------
  val consoleQuery = output.writeStream
    .format("console")
    .option("truncate", false)
    .option("numRows", 20)
    .outputMode("append")
    .start()

  println("Streaming prediction started. PostgreSQL + Console + Email alerts active.")

  pgQuery.awaitTermination()
  consoleQuery.awaitTermination()
}

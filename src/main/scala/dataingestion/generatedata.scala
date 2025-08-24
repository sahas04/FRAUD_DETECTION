package dataingestion

import org.apache.spark.sql.SparkSession
import scala.util.Random
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId}
import java.time.temporal.ChronoUnit

object FraudDatasetJSON extends App {
  val spark = SparkSession.builder()
    .appName("FraudDatasetJSON")
    .master("local[*]") // remove when running on cluster
    .getOrCreate()

  import spark.implicits._

  val random = new Random()

  // Possible categorical values
  val locations = Seq("NY", "CA", "TX", "FL", "NJ", "WA", "IL", "MA")
  val devices = Seq("Mobile", "Web", "ATM", "POS")
  val merchants = Seq("Grocery", "Electronics", "Travel", "Clothing", "Restaurants", "Fuel")

  // Current timestamp
  val now = LocalDateTime.now()

  // Generate 50,000 rows
  val data = (1 to 50000).map { i =>
    val transactionId = s"txn$i"
    val userId = random.nextInt(5000) + 1
    val amount = BigDecimal(random.nextDouble() * 10000)
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      .toDouble
    val location = locations(random.nextInt(locations.length))
    val device = devices(random.nextInt(devices.length))
    val merchant = merchants(random.nextInt(merchants.length))

    // Random timestamp within last 30 days
    val randMinutes = random.nextInt(30 * 24 * 60) // 30 days in minutes
    val timestamp = Timestamp.valueOf(now.minusMinutes(randMinutes.toLong))

    // ---- Fraud probability logic ----
    var fraudProb = 0.05 // base
    if (amount > 7000) fraudProb += 0.20
    else if (amount > 3000) fraudProb += 0.10
    if (device == "Mobile" || device == "ATM") fraudProb += 0.15
    if (merchant == "Electronics" || merchant == "Travel") fraudProb += 0.15
    if (location == "TX" || location == "FL") fraudProb += 0.05
    fraudProb = math.min(fraudProb, 0.85)
    val label = if (random.nextDouble() < fraudProb) 1 else 0

    (transactionId, userId, amount, location, device, merchant, timestamp, label)
  }

  val df = data.toDF(
    "transactionId", "userId", "amount", "location",
    "device", "merchant", "timestamp", "label"
  )

  // Save directly as JSON in HDFS
  val outputPath = "fraud_json_output"
  df.write.mode("overwrite").json(outputPath)

  // Quick class balance check
  val counts = df.groupBy("label").count().collect()
  println("✅ Fraud dataset generated and saved as JSON at " + outputPath)
  counts.foreach(r => println(s"Label ${r(0)} Count: ${r(1)}"))

  spark.stop()
}
package dataingestion

import org.apache.spark.sql.SparkSession
import scala.util.Random
import java.sql.Timestamp
import java.time.LocalDateTime
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.Properties

object kafkaproducer extends App {

  val spark = SparkSession.builder()
    .appName("FraudDatasetKafka")
    .master("local[*]")
    .getOrCreate()

  val random = new Random()

  val locations = Seq("NY", "CA", "TX", "FL", "NJ", "WA", "IL", "MA")
  val devices = Seq("Mobile", "Web", "ATM", "POS")
  val merchants = Seq("Grocery", "Electronics", "Travel", "Clothing", "Restaurants", "Fuel")

  val now = LocalDateTime.now()

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  println("Starting Kafka Producer... Sending transactions to 'mlevents1' topic\n")

  (1 to 20).foreach { i =>
    val transactionId = s"txn$i"
    val userId = random.nextInt(5000) + 1
    val amount = BigDecimal(random.nextDouble() * 10000)
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      .toDouble
    val location = locations(random.nextInt(locations.length))
    val device = devices(random.nextInt(devices.length))
    val merchant = merchants(random.nextInt(merchants.length))
    val timestamp = Timestamp.valueOf(now.minusMinutes(random.nextInt(1000)))


    var fraudProb = 0.05
    if (amount > 7000) fraudProb += 0.20
    else if (amount > 3000) fraudProb += 0.10
    if (device == "Mobile" || device == "ATM") fraudProb += 0.15
    if (merchant == "Electronics" || merchant == "Travel") fraudProb += 0.15
    if (location == "TX" || location == "FL") fraudProb += 0.05
    fraudProb = math.min(fraudProb, 0.85)

    val label = if (random.nextDouble() < fraudProb) 1 else 0

    val json =
      s"""
         |{
         | "transactionId": "$transactionId",
         | "userId": $userId,
         | "amount": $amount,
         | "location": "$location",
         | "device": "$device",
         | "merchant": "$merchant",
         | "timestamp": "$timestamp",
         | "label": $label
         |}
       """.stripMargin.replaceAll("\n", "")


    val record = new ProducerRecord[String, String]("mlevents1", transactionId, json)
    producer.send(record)
    producer.flush()


    println(s"Sent Transaction -> ID: $transactionId, User: $userId, Amount: $amount, Location: $location, Device: $device, Merchant: $merchant, Label: $label, Timestamp: $timestamp")
    Thread.sleep(500)
  }

  println("\nAll transactions sent successfully!")
  producer.close()
  spark.stop()
}

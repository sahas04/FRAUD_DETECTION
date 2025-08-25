package project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}

object TrainGBTFraud extends App {
  val spark = SparkSession.builder()
    .appName("TrainGBTFraud")
    .master("local[*]") // remove in cluster
    .getOrCreate()

  import spark.implicits._

  println("⏳ Loading dataset from HDFS...")
  var df = spark.read.json("hdfs://localhost:9000/data/fraud_json_output1")
  println(s"Dataset loaded. Total rows: ${df.count()}")

  // ---------------------------
  // Transform 'amount' in-place
  // ---------------------------
  println("⏳ Applying log-transform to amount column...")
  df = df.withColumn("amount", log1p($"amount"))
  println("Log-transform applied.")

  // ---------------------------
  // Compute class weights
  // ---------------------------
  println("⏳ Computing class weights for imbalance...")
  val total = df.count().toDouble
  val fraudCount = df.filter($"label" === 1).count().toDouble
  val nonFraudCount = total - fraudCount
  val weightedDF = df.withColumn("weight",
    when($"label" === 1, nonFraudCount / total)
      .otherwise(fraudCount / total)
  )
  println(s"Class weights added. Fraud ratio: ${fraudCount/total} Non-fraud ratio: ${nonFraudCount/total}")

  // ---------------------------
  // Split train/test
  // ---------------------------
  println("⏳ Splitting dataset into train/test...")
  val Array(trainDF, testDF) = weightedDF.randomSplit(Array(0.8, 0.2), seed = 42)
  println(s"Training rows: ${trainDF.count()}, Test rows: ${testDF.count()}")

  // ---------------------------
  // Index categorical columns
  // ---------------------------
  println("Indexing categorical columns...")
  val indexers = Array("location", "device", "merchant").map { colName =>
    println(s"    ➤ Indexing column: $colName")
    new StringIndexer()
      .setInputCol(colName)
      .setOutputCol(colName + "_idx")
      .setHandleInvalid("keep")
      .fit(df)
  }

  // ---------------------------
  // Assemble features
  // ---------------------------
  println("Assembling features into vector...")
  val assembler = new VectorAssembler()
    .setInputCols(Array("userId", "amount", "location_idx", "device_idx", "merchant_idx"))
    .setOutputCol("features")
  println("Features assembled.")

  // ---------------------------
  // Configure GBTClassifier
  // ---------------------------
  println("Setting up GBT Classifier with weighted classes...")
  val gbt = new GBTClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setWeightCol("weight")   // use weights
    .setMaxIter(100)
    .setMaxDepth(5)
    .setStepSize(0.1)
    .setSubsamplingRate(0.8)
  println("GBT Classifier configured.")

  // ---------------------------
  // Pipeline and train
  // ---------------------------
  println("Creating pipeline and training model...")
  val pipeline = new Pipeline().setStages(indexers ++ Array(assembler, gbt))
  val model = pipeline.fit(trainDF)
  println("Model training completed.")

  // ---------------------------
  // Save model to HDFS
  // ---------------------------
  println("Saving model to HDFS...")
  val modelPath = "hdfs://localhost:9000/models/gbt_fraud_model1"
  model.write.overwrite().save(modelPath)
  println(s"Model saved at $modelPath")

  // ---------------------------
  // Predictions
  // ---------------------------
  println("Making predictions on test set...")
  val predictions = model.transform(testDF)
  println("Predictions completed.")

  // ---------------------------
  // Evaluate metrics
  // ---------------------------
  println("Evaluating model metrics...")
  val binaryEvaluator = new BinaryClassificationEvaluator()
    .setLabelCol("label")
    .setRawPredictionCol("prediction")
    .setMetricName("areaUnderROC")
  val auc = binaryEvaluator.evaluate(predictions)
  println(f"AUROC: $auc%.4f")

  val accuracyEvaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")
  val accuracy = accuracyEvaluator.evaluate(predictions)

  val precisionEvaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("precisionByLabel")
  val precision = precisionEvaluator.evaluate(predictions)

  val recallEvaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("recallByLabel")
  val recall = recallEvaluator.evaluate(predictions)

  val f1Evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("f1")
  val f1 = f1Evaluator.evaluate(predictions)

  println(f"Accuracy : $accuracy%.4f")
  println(f"Precision: $precision%.4f")
  println(f"Recall   : $recall%.4f")
  println(f"F1 Score : $f1%.4f")

  println("✅ All steps completed successfully.")
  spark.stop()
}

// Basic Spark imports
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._

// Spark SQL Cassandra imports
import org.apache.spark.sql
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._

// Spark Streaming + Kafka imports
import kafka.serializer.StringDecoder // this has to come before streaming.kafka import
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

// Cassandra Java driver imports
import com.datastax.driver.core.{ Session, Cluster, Host, Metadata }
import com.datastax.spark.connector.streaming._
import scala.collection.JavaConversions._

// Date import for processing logic
import java.util.Date

//Mllib
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils

object EventStream {

  def main(args: Array[String]) {

    // read the configuration file
    val sparkConf = new SparkConf().setAppName("EventStream")

    // get the values we need out of the config file
    val kafka_broker = "localhost:9092"
    val kafka_topic = "test"
    val cassandra_host = sparkConf.get("spark.cassandra.connection.host"); //cassandra host
    val cassandra_user = sparkConf.get("spark.cassandra.auth.username");
    val cassandra_pass = sparkConf.get("spark.cassandra.auth.password");

    // connect directly to Cassandra from the driver to create the keyspace
    val cluster = Cluster.builder().addContactPoint(cassandra_host).withCredentials(cassandra_user, cassandra_pass).build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS ic_example WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS ic_example.word_count (word text, ts timestamp, count int, PRIMARY KEY(word, ts)) ")
    session.execute("TRUNCATE ic_example.word_count")
    session.close()

    // Create spark streaming context with 5 second batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set[String](kafka_topic)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafka_broker)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Create the processing logic
    // the spark processing isn't actually run until the streaming context is started
    // it will then run once for each batch interval

    // Get the lines, split them into words, count the words and print
    val wordCounts = messages.map(_._2) // split the message into lines
      .flatMap(_.split(" ")) //split into words
      .filter(w => (w.length() > 0)) // remove any empty words caused by double spaces
      .map(w => (w, 1L)).reduceByKey(_ + _) // count by word
      .map({ case (w, c) => (w, new Date().getTime(), c) }) // add the current time to the tuple for saving

    wordCounts.print() //print it so we can see something is happening

    // insert the records from  rdd to the ic_example.word_count table in Cassandra
    // SomeColumns() is a helper class from the cassandra connector that allows the fields of the rdd to be mapped to the columns in the table
    wordCounts.saveToCassandra("ic_example", "word_count", SomeColumns("word" as "_1", "ts" as "_2", "count" as "_3"))

    //ML LIB
    // Get the results using spark SQL
    /*
    val sc = new SparkContext(sparkConf) // create a new spark core context
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "../data/mllib/sample_libsvm_data.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / (testData.count() + 0.0)
    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    println("Test Error= " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    // Save and load model
    //model.save(sc, "target/tmp/myRandomForestClassificationModel")
    //val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
		*/
		
    // Now we have set up the processing logic it's time to do some processing
    ssc.start() // start the streaming context
    ssc.awaitTermination() // block while the context is running (until it's stopped by the timer)
    ssc.stop() // this additional stop seems to be required

    // Get the results using spark SQL
    //val sc = new SparkContext(sparkConf) // create a new spark core context
    //val rdd1 = sc.cassandraTable("ic_example", "word_count")
    //rdd1.take(100).foreach(println)
    //sc.stop()
  }
}

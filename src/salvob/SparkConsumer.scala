package salvob

/**
  * Created by salvob on 12/05/2016.
  */
import org.apache.spark.streaming.kafka.{KafkaUtils, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
//import org.json4s.DefaultFormats

import titandb._

object SparkConsumer {

  //needed to make the format conversion to case class - import DefaultFormats from lift-jsons
  import net.liftweb.json.{DefaultFormats, _}
  implicit val formats = DefaultFormats

  private var numMsgCollected = 0L
  private var partitionNum = 0
  private var numComment = 0

  def main(args: Array[String]): Unit = {

    val streamingRateSeconds = 5
    val durationMilliSecs: Int = 300000 // 5 minutes

    //The appName parameter is a name for your application to show on the cluster UI.
    val conf = new SparkConf().setAppName("Kafka's streaming")
    //conf.setMaster("yarn-client")
    conf.setMaster("local[2]")
    // A StreamingContext object can be created from a SparkConf object.
    val sc = new SparkContext(conf)
    //less verbose
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(streamingRateSeconds))

    val zkQuorum = "localhost:2181" //zookeeper IP address
    val userName = "sam" // change to your user
    val inputGroup = userName + "Group" //ConsumerGroup
    val topic:String = "posts"
    val topicMap = Map(topic -> 1)

    //TITANDB connection
    val confPath: String = System.getProperty("user.dir")+"/src/resources/"+"titan/config/titan-cassandra-es.properties"
    val conn = new TitanModule(confPath)
    System.out.println("CONNESSIONE AVVENUTA")

    //Define the input sources by creating input DStreams.
    //Discretized Stream or DStream is the basic abstraction provided by Spark Streaming.
    // It represents a continuous stream of data
    //a DStream is represented by a continuous series of RDDs

    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, inputGroup, topicMap)


    val count = kafkaStream.count()

    //SPARKSQL

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //for every message Kafka, get its second element (the first are info about the kafka status)
    kafkaStream.foreachRDD((rdd._2, time) => {
      //if we received any messages into Kafka stream, analyse them

      val count = rdd.count()
      if (count > 0){
        //save the RDD as file
        val outputRDD = rdd.repartition(1)
        outputRDD.saveAsTextFile("output/posts_" + time.milliseconds.toString)

        rdd.foreach(record => {
          //println(record)

          //parser JSON with sparkSQL
          // Create the DataFrame
          val df = sqlContext.read.json(record)

          // Show the content of the DataFrame
          df.show()

          //JSON
          /*
          http://stackoverflow.com/questions/4169153/what-is-the-most-straightforward-way-to-parse-json-in-scala/4169292#4169292
           */
          val parsedJson = parse(record)

        })

        //we stop after 100 message collected
        numMsgCollected += count
        if (numMsgCollected > 100) {
          //let's write in the file
          import java.io._

          val file = "Stats.txt"
          val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
          writer.write("Post collected: "+numMsgCollected + "\n")
          writer.close()

          System.exit(0)
        }
      }

    })

    // Start streaming
    //Start receiving data and processing it using streamingContext.start().
    ssc.start()

    ssc.awaitTerminationOrTimeout(300000)
  }
}
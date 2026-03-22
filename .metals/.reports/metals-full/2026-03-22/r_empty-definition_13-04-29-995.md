error id: file:///C:/Users/Admin/rust-advanced/kv-graph-pipeline/kv-graph-spark/src/main/scala/KvGraphJob.scala:client5.
file:///C:/Users/Admin/rust-advanced/kv-graph-pipeline/kv-graph-spark/src/main/scala/KvGraphJob.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/functions/org/apache/hc/client5.
	 -org/apache/spark/sql/types/org/apache/hc/client5.
	 -org/apache/hc/client5.
	 -scala/Predef.org.apache.hc.client5.
offset: 142
uri: file:///C:/Users/Admin/rust-advanced/kv-graph-pipeline/kv-graph-spark/src/main/scala/KvGraphJob.scala
text:
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hc.c@@lient5.http.fluent.Request

object KvGraphJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("kv-graph")
      .getOrCreate()

    import spark.implicits._

    val schema = new StructType()
      .add("user_id", StringType)
      .add("movie_id", StringType)
      .add("action", StringType)

    val kafkaBootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "kafka-0.kafka:9092,kafka-1.kafka:9092")
    val kvdalEndpoint = sys.env.getOrElse("KVDAL_URL", "http://kvdal:8080/append")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", "user-interactions")
      .option("startingOffsets", "latest")
      .load()

    val parsed = df
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", schema).as("data"))
      .select("data.*")

    val edges = parsed.map { row =>
      val user = row.getAs[String]("user_id")
      val movie = row.getAs[String]("movie_id")
      val action = row.getAs[String]("action")

      val key = s"$user:$action"
      val value = movie

      (key, value)
    }.toDF("key", "value")

    edges.writeStream.foreachBatch { (batchDF, _) =>

      batchDF.foreachPartition { partition =>

        partition.foreach { row =>

          val key = row.getString(0)
          val value = row.getString(1)

          val json =
            s"""{"key":"$key","value":"$value"}"""

          try {
            Request.post(kvdalEndpoint)
              .bodyString(json, org.apache.hc.core5.http.ContentType.APPLICATION_JSON)
              .execute()
              .discardContent()
          } catch {
            case e: Exception =>
              println(s"Failed to send: $e")
          }
        }
      }

    }
    .start()
    .awaitTermination()
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 
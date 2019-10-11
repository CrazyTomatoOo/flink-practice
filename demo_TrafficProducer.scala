package demo

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random

object TrafficProducer {
  class Data(eID: String, timestamp: Long, direction: String, vID: String, prov: String, vC: String){
    override def toString: String = {
      this.eID+","+this.timestamp+","+this.direction+","+this.vID+","+this.prov+","+this.vC
    }
  }

  def main(args: Array[String]): Unit = {
    val properties: Properties = new Properties()
    properties.put("bootstrap.servers", "manager:9092,worker1:9092,worker2:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](properties)
    val eID = List("D109786C45", "D109786C20",
      "D109786C11", "D109786C90",
      "D109786C80", "D109786C35",
      "D109786C09", "D109786C67")
    val direction = List("NS", "SN", "EW", "WE")
    val prov = List("京", "豫", "鲁", "晋", "鄂", "沪", "浙", "苏", "新", "黑")
    val vC = List("B", "G", "Y")
    while (1 <= 100) {
      val a = prov(Random.nextInt(10))
      val data =new Data(eID(Random.nextInt(8)),
        System.currentTimeMillis(),
        direction(Random.nextInt(4)),
        a + (Random.nextInt(25) + 65).toChar + "%04d".format(Random.nextInt(10000)),
        a,
        vC(Random.nextInt(3))
      )

      val record = new ProducerRecord[String,String]("test", null, System.currentTimeMillis(),null, data.toString)
      producer.send(record)
      println(data)
      Thread.sleep(Random.nextInt(3000)+200)
    }
  }
}

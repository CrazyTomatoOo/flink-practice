package demo

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object ClickProducer {
  def main(args: Array[String]): Unit = {
    val properties: Properties = new Properties()
    properties.put("bootstrap.servers", "manager:9092,worker1:9092,worker2:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)
    val userID = List("user1", "user2", "user3", "user4", "user5", "user6")
    val wedsite = List("www.baidu.com", "www.google.com",
      "www.qq.com", "www.163.com",
      "www.sina.com.cn", "http://www.sohu.com",
      "www.bilibili.com", "www.douyu.com")
    while (1 < 100) {
      val data = Data(userID(Random.nextInt(5)), System.currentTimeMillis(), wedsite(Random.nextInt(7)))
      val record = new ProducerRecord[String, String]("click", null, data.time, null, data.toString)
      producer.send(record)
      println(data)
      Thread.sleep(Random.nextInt(3000) + 200)
    }
  }

  case class Data(
                   user: String,//用户名
                   time: Long,//点击时间
                   website: String//网站
                 ){
    override def toString: String = {
      this.user+" "+this.time+" "+this.website
    }
  }

}


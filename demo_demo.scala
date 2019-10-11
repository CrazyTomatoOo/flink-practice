package demo

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object demo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", "manager:2181,worker1:2181,worker2:2181")
    kafkaProps.setProperty("bootstrap.servers", "manager:9092,worker1:9092,worker2:9092")
    kafkaProps.setProperty("group.id", "flinkdemo3")

    val consumer = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), kafkaProps)
    consumer.setStartFromEarliest()
    val transaction = env.addSource(consumer).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[String] {
      override def extractAscendingTimestamp(t: String): Long = System.currentTimeMillis()-3000L
    })

    transaction
      .map(x => x.replaceAll(",", " ")
        .split(" "))
      .map(x => (x(3), 1))
      .keyBy(0)
      .window(EventTimeSessionWindows.withGap(Time.seconds(3)))
      .sum(1)
      .print()
    env.execute()
  }
}

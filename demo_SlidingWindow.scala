package demo

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}


object SlidingWindow {
  case class Data(time:Long,fx:String){
     val this.time=time
    val this.fx=fx
  }
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.enableCheckpointing(1000)
    //    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", "manager:2181,worker1:2181,worker2:2181")
    kafkaProps.setProperty("bootstrap.servers", "manager:9092,worker1:9092,worker2:9092")
    kafkaProps.setProperty("group.id", "flinkdemo2")

    val consumer = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), kafkaProps)
    consumer.setStartFromEarliest()

    val transaction = env.addSource(consumer)
    transaction
      .map(x => x.split(","))
      .map(x=>Data(x(1).toLong,x(2)))
      .assignAscendingTimestamps(_.time)
      .map(x => (x.fx, 1))
      .keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(15)))
      .sum(1)
      .print()
    env.execute()
  }
}
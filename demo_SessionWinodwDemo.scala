package demo

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SessionWinodwDemo {
    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      //    env.enableCheckpointing(1000)
      //    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

      val kafkaProps = new Properties()
      kafkaProps.setProperty("zookeeper.connect", "manager:2181,worker1:2181,worker2:2181")
      kafkaProps.setProperty("bootstrap.servers", "manager:9092,worker1:9092,worker2:9092")
      kafkaProps.setProperty("group.id", "flinkdemo3")

      val consumer = new FlinkKafkaConsumer[String]("click", new SimpleStringSchema(), kafkaProps)
      consumer.setStartFromEarliest()
      val transaction = env.addSource(consumer)
      transaction
        .map(x => x.split(" "))
        .map(x=>(x(0),x(1).toLong,x(2)))
        .assignAscendingTimestamps(_._2)
        .map(x => InPut(x._3))
        .keyBy(_.s)
        .window(EventTimeSessionWindows.withGap(Time.seconds(3)))
          .apply(new WindowFunction[InPut,OutPut,String,TimeWindow] {
            override def apply(key: String, window: TimeWindow, input: Iterable[InPut], out: Collector[OutPut]): Unit = {
              out.collect(OutPut(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(window.getStart),new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(window.getEnd),input.toIterator.next().s,input.size))
            }
          }).print()
      env.execute()
    }
  case class InPut(
                    s:String,
                    n:Int=1
                  )
  case class OutPut(start:String,
                    end:String,
                    website:String,
                    click:Int
                   )
}


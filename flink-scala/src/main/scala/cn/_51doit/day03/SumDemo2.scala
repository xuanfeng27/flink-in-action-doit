package cn._51doit.day03

import org.apache.flink.streaming.api.scala._

object SumDemo2 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //Source
    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)

    //Transformation开始
    val words: DataStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DataStream[(String, Int)] = words.map(w => (w, 1))

    //按照指定的字段进行分区（使用的是Hash分区方式）
    val keyed: KeyedStream[(String, Int), String] = wordAndOne.keyBy(t => t._1)

    //将key相同的数据进行聚合
    val res: DataStream[(String, Int)] = keyed.sum("_2")
    //Transformation结束

    //调用Sink
    res.print()

    //执行
    env.execute()
  }


}

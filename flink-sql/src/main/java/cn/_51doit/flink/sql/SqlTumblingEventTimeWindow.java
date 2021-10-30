package cn._51doit.flink.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class SqlTumblingEventTimeWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //为了触发方便，并行度设置为1
        env.setParallelism(1);

        //如果希望能够容错，要开启checkpoint
        env.enableCheckpointing(10000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //11000,u1,p1,5
        //12000,u1,p1,5
        //2000,u2,p1,3
        //3000,u1,p1,5
        //19999,u2,p1,3
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Row> rowDataStream = socketTextStream.map(
                new MapFunction<String, Row>() {
                    @Override
                    public Row map(String line) throws Exception {
                        String[] fields = line.split(",");
                        Long time = Long.parseLong(fields[0]);
                        String uid = fields[1];
                        String pid = fields[2];
                        Double money = Double.parseDouble(fields[3]);
                        return Row.of(time, uid, pid, money);

                    }
                }).returns(Types.ROW(Types.LONG, Types.STRING, Types.STRING, Types.DOUBLE));

        //提取数据中的EventTime并生成WaterMark
        DataStream<Row> waterMarksRow = rowDataStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Row row) {
                        return (long) row.getField(0);
                    }
                });
        //将DataStream注册成表并指定schema信息
        //$("etime").rowtime()，将数据流中的EVENT Time取名为etime
        tableEnv.createTemporaryView("t_orders", waterMarksRow, $("time"), $("uid"), $("pid"), $("money"), $("etime").rowtime());
        //tableEnv.registerDataStream("t_orders", waterMarksRow, "time, uid, pid, money, rowtime.rowtime");


        //将同一个滚动窗口内，相同用户id的money进行sum操作(没有取出窗口的起始时间、结束时间)
        //String sql = "SELECT uid, SUM(money) total_money FROM t_orders GROUP BY TUMBLE(etime, INTERVAL '10' SECOND), uid";



        //使用SQL实现按照EventTime划分滚动窗口聚合
        String sql = "SELECT uid, SUM(money) total_money, TUMBLE_START(etime, INTERVAL '10' SECOND) as win_start, " +
                "TUMBLE_END(etime, INTERVAL '10' SECOND) as win_end " +
                "FROM t_orders GROUP BY TUMBLE(etime, INTERVAL '10' SECOND), uid";


        //waterMarksRow.keyBy(t -> t.getField(1)).window(TumblingEventTimeWindows.of(Time.seconds(10)))

                Table table = tableEnv.sqlQuery(sql);
        //使用TableEnv将table转成AppendStream
        DataStream<Row> result = tableEnv.toAppendStream(table, Row.class);
        result.print();
        env.execute();
    }

}

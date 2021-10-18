package cn._51doit.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 将两个数据流（类型可以不一致）connect到一起，返回一个新的DataStream，保存各自数据流的类型
 *
 * 最大的用处：可以让两个数据流共享状态
 *
 */
public class ConnectDemo {

    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> nums = lines1.map(Integer::parseInt).setParallelism(2);

        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        ConnectedStreams<Integer, String> connectedStreams = nums.connect(lines2);

        SingleOutputStreamOperator<String> resStream = connectedStreams.map(new CoMapFunction<Integer, String, String>() {

            //定义共享状态（变量，存取数据，可以容错）

            //对第一个数量处理的方法
            @Override
            public String map1(Integer value) throws Exception {
                return value.toString();
            }

            //对第二个数量处理的方法
            @Override
            public String map2(String value) throws Exception {
                return value.toUpperCase();
            }
        });

        resStream.print();

        env.execute();




    }
}

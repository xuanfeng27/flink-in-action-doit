package cn._51doit.day02.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * map算子的功能：做映射，输入一个元素，应用完map的运算逻辑后返回一个元素
 * map算子底层调用的transform方法
 */
public class MapDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

//        SingleOutputStreamOperator<String> upperStream = lines.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String value) throws Exception {
//                return value.toUpperCase();
//            }
//        });

        SingleOutputStreamOperator<String> upperStream = lines.transform("MyMap", TypeInformation.of(String.class), new MyStreamMap());

        upperStream.print();

        env.execute();


    }

    private static class MyStreamMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {

        /**
         * processElement来一条元素调用一次该方法
         * 进入该算子的数据被封装到StreamRecord中了
         * @param element
         * @throws Exception
         */
        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            //取出输入的数据
            String in = element.getValue();
            //将输入的数据变大写
            String upper = in.toUpperCase();
            //将数据输出
            output.collect(new StreamRecord<>(upper));
        }
    }

}

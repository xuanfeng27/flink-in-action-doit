package cn._51doit.day02.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class MyMapDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //计算逻辑
        MapFunction<String, Tuple2<String, Integer>> mapFunction = new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        };

        //TypeInformation.of(String.class) 或 TypeInformation.of(Boy.class)
        //TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}
        //解决输出数据有泛型嵌套的问题Tuple2<String, Integer>
        SingleOutputStreamOperator<Tuple2<String, Integer>> resStream = lines.transform("MyMap", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}), new MyStreamMap<>(mapFunction));

        resStream.print();

        env.execute();

    }


    private static class MyStreamMap<I, O> extends AbstractUdfStreamOperator<O, MapFunction<I, O>> implements OneInputStreamOperator<I, O> {

        public MyStreamMap(MapFunction<I, O> function) {
            super(function);
        }

        @Override
        public void processElement(StreamRecord<I> element) throws Exception {
            I in = element.getValue();
            //应用外部传入的计算逻辑，对当前数据进行计算
            O out = userFunction.map(in);
            output.collect(element.replace(out));
        }
    }
}

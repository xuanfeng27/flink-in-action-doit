package cn._51doit.day02.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * flatMap是将数据先map在打平，输入一个元素，可以输出0到多个元素
 *
 *  调用DataStream的transformation方法传入java的Lambda表达式，
 *  如果返回的数据封装到一个带有泛型的包装类中（TupleN）
 *  或是使用Collector将数据输出，必须加上returns
 *
 */
public class FlatMapDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

//        SingleOutputStreamOperator<String> words = lines.flatMap((String line, Collector<String> out) -> {
//            for (String word : line.split(" ")) {
//                out.collect(word);
//            }
//        }).returns(Types.STRING);

        SingleOutputStreamOperator<String> words = lines.transform("MyFlatMap", TypeInformation.of(String.class), new MyStreamFlatMap());

        words.print();

        env.execute();


    }

    private static class MyStreamFlatMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {

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
            //对数据进行切分压平
            String[] words = in.split(" ");
            //循环
            for (String word : words) {
                output.collect(new StreamRecord<>(word));
            }
        }
    }

}

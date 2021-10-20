package cn._51doit.day02.transformations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * 实时对数据进行过滤，调用filter方法，传入过滤逻辑，如果返回true就留下
 */
public class FilterDemo {

    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //SingleOutputStreamOperator<String> filtered = lines.filter(line -> !line.startsWith("error"));

        SingleOutputStreamOperator<String> filtered = lines.transform("MyFilter", TypeInformation.of(String.class), new MyStreamFilter());

        filtered.print();

        env.execute();
    }

    private static class MyStreamFilter extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {

        //每输入一条数据调用一次该方法
        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            //获取数据的数据
            String in = element.getValue();
            //判断该数据不是已error开头
            boolean flag = !in.startsWith("error");
            if (flag) {
                //输出不是以error开头的数据
                output.collect(element);
            }
        }
    }
}

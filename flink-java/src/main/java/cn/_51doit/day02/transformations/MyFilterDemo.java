package cn._51doit.day02.transformations;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * 实时对数据进行过滤，调用filter方法，传入过滤逻辑，如果返回true就留下
 */
public class MyFilterDemo {

    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        FilterFunction<String> filterFunction = new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !value.startsWith("error");
            }
        };

        SingleOutputStreamOperator<String> filtered = lines.transform("MyFilter", TypeInformation.of(String.class), new MyStreamFilter<>(filterFunction));

        filtered.print();

        env.execute();
    }

    private static class MyStreamFilter<I> extends AbstractUdfStreamOperator<I, FilterFunction<I>> implements OneInputStreamOperator<I, I> {


        public MyStreamFilter(FilterFunction<I> userFunction) {
            super(userFunction);
        }

        @Override
        public void processElement(StreamRecord<I> element) throws Exception {
            I in = element.getValue();
            boolean flag = userFunction.filter(in);
            if (flag) {
                //输出
                output.collect(element);
            }
        }
    }

}

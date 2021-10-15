package cn._51doit.day02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * 自定义一个printSink，就是将数据输出到控制台
 */
public class MyPrintSink {

    public static void main(String[] args) throws Exception {


        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        lines.addSink(new MyAdvPrintSinkFunction());

        env.execute();


    }

    private static class MyAdvPrintSinkFunction extends RichSinkFunction<String> {

        public MyAdvPrintSinkFunction() {
            System.out.println("MyAdvPrintSinkFunction的构造方法执行了~~~");
        }

        private int taskPrefix;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open方法被调用了~~~~~~");
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            taskPrefix = indexOfThisSubtask + 1;
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            //System.out.println("invoke方法被调用了######");
            System.out.println(taskPrefix + " > " + value);
        }

        @Override
        public void close() throws Exception {
            System.out.println("close方法被调用了######");
        }
    }

//    private static class MyPrintSinkFunction implements SinkFunction<String> {
//
//        /**
//         * 每来一条数据调用一次invoke方法
//         * @param value
//         * @param context
//         * @throws Exception
//         */
//        @Override
//        public void invoke(String value, Context context) throws Exception {
//            System.out.println(value);
//        }
//    }
}

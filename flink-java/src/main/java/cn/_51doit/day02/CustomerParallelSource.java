package cn._51doit.day02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;


/**
 * 自定义多并行的Source，即Source的并行度只可以是1到多个
 *
 *
 */
public class CustomerParallelSource {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> nums = env.addSource(new ParallelSourceFunc());

        System.out.println("自定义ParallelSourceFunc得到的DataStream的并行度为：" + nums.getParallelism());

        nums.print();

        env.execute();


    }

    /**
     * 如果实现了ParallelSourceFunction接口或继承了RichParallelSourceFunction抽象里，那么就是多并行的Source
     */
    private static class ParallelSourceFunc extends RichParallelSourceFunction<String> {

        private boolean flag = true;

        public ParallelSourceFunc() {
            System.out.println("构造方法执行了！！！！！！！！！！！");
        }

        /**
         * 先调用Open方法
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(indexOfThisSubtask + ": Open方法被调用***********");
        }

        /**
         * open方法调用完后再调用run方法
         * run方法task启动后会执行一次
         * 如果run方法一直不退出，就是一个无限的数据流
         * 如果数据读取完了，run方法退出，就是一个有限的数据流，Source退出，job也停止了
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(indexOfThisSubtask +" : Run方法被调用了￥￥￥￥￥￥￥￥");
            Random random = new Random();
            //获取当前SubTask的Index

            while (flag) {
                int i = random.nextInt(100);
                ctx.collect(indexOfThisSubtask + " --> " + i);
                Thread.sleep(1000);
            }
        }

        /**
         * task cancel会执行一次
         */
        @Override
        public void cancel() {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(indexOfThisSubtask + " : Cancel方法被调用了~~~~~");
            flag = false;
        }

        /**
         * 如果人为将job cancel先调用cancel方法再调用close方法
         * 如果没有将job人为的cancel，任务停掉前一定会调用close方法
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(indexOfThisSubtask + " : Close方法被调用@@@@@@@@@");
        }

    }

}

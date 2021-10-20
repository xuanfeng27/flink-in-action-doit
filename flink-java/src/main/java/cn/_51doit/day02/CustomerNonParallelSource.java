package cn._51doit.day02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


/**
 * 自定义非并行的Source，即Source的并行度只为1
 *
 *   如果实现了SourceFunction接口或继承了RichSourceFunction抽象类，得到的DataStream为只有一个并行
 */
public class CustomerNonParallelSource {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<Integer> nums = env.addSource(new NonParallelSourceFunc());

        System.out.println("自定义NonParallelSourceFunc得到的DataStream的并行度为：" + nums.getParallelism());

        nums.print();

        env.execute();


    }

    private static class NonParallelSourceFunc implements SourceFunction<Integer> {

        /**
         * run方法task启动后会执行一次
         * 如果run方法一直不退出，就是一个无限的数据流
         * 如果数据读取完了，run方法退出，就是一个有限的数据流，Source退出，job也停止了
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            System.out.println("Run方法被调用了~~~~~");
            for (int i = 0; i < 100; i++) {
                //Source产生的数据使用SourceContext将数据输出
                ctx.collect(i);
            }

        }

        /**
         * task cancel会执行一次
         */
        @Override
        public void cancel() {
            System.out.println("Cancel方法被调用了~~~~~");
        }
    }

}

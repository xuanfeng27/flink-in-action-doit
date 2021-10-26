package cn._51doit.day09;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * SideOutput（测流输出、旁路输出）
 * 将整个数量流按照指定类型，打上一到多个标签，然后按照指定的标签将数据筛选出来，再对数据进行处理
 * <p>
 * SideOutput VS Filter (测流输出与过滤的区别)
 * SideOutput底层是给数据关联上标签，然后根据标签获取指定类型的数据
 * Filter是将不符合条件的数据过滤掉，保留符合条件的数据，如果想要保留多种类型的数据，需要过滤多次，过滤效果低
 * <p>
 * 需求：将数据中的偶数、奇数、和字符串打上标签
 */
public class SideOutputDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //1
        //2
        //abc
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //定义标签(奇数)
        OutputTag<Integer> oddTag = new OutputTag<Integer>("odd-tag") {
        };
        //偶数的标签
        OutputTag<Integer> evenTag = new OutputTag<Integer>("even-tag") {
        };
        //字符串的标签
        OutputTag<String> strTag = new OutputTag<String>("str-tag") {
        };

        //对应主流来说，数据分类两种（打标签和未打标签）
        SingleOutputStreamOperator<String> mainStream = lines.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String in, Context ctx, Collector<String> out) throws Exception {

                try {
                    int num = Integer.parseInt(in);
                    if (num % 2 == 0) {
                        //偶数
                        //对打标签的数据进行输出
                        ctx.output(evenTag, num);
                    } else {
                        //对打标签的数据进行输出
                        ctx.output(oddTag, num);
                    }
                } catch (NumberFormatException e) {
                    //e.printStackTrace();
                    //字符串
                    //对打标签的数据进行输出
                    ctx.output(strTag, in);
                }

                //输出未打标签的数据
                out.collect(in);
            }
        });

        //从主流中取出打标签的数据
        DataStream<Integer> oddStream = mainStream.getSideOutput(oddTag);
        oddStream.print("odd ");

        //从主流中取出打标签的数据
        DataStream<String> strStream = mainStream.getSideOutput(strTag);
        strStream.print("str ");

        //多未打标签的数据进行处理
        mainStream.print("main ");


        env.execute();
    }
}

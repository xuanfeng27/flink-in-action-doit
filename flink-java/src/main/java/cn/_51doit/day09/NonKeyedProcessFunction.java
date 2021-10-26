package cn._51doit.day09;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink的ProcessFunction，是Flink底层的算子，可以访问Flink底层的属性和方法
 *
 * 1.处理数据（elements）
 * 2.使用状态（KeyedStream）
 * 3.使用定时器（KeyedStream）
 *
 * 现在演示的是没有keyBy的DataStream，只能用了处理数据
 *
 * 例子：将json字符串转成json对象
 * {"name":"tom", "age":18, "fv":999.99}
 * {"name":"jerry", "age":12, "fv":999.99
 */
public class NonKeyedProcessFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //如果先使用map，出现异常将异常捕获，然后调用filter方法将有问题的数据过滤掉
        //现在我想使用ProcessFunction，再一个方法中实现全部的功能
        SingleOutputStreamOperator<User> res = lines.process(new ProcessFunction<String, User>() {
            @Override
            public void processElement(String value, Context ctx, Collector<User> out) throws Exception {

                try {
                    User user = JSON.parseObject(value, User.class);
                    out.collect(user);
                } catch (Exception e) {
                    //e.printStackTrace();
                    //有问题的数据单独记录下来
                }
            }
        });

        res.print();

        env.execute();


    }


    public static class User {

        private String name;
        private Integer age;
        private Double fv;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public Double getFv() {
            return fv;
        }

        public void setFv(Double fv) {
            this.fv = fv;
        }

        @Override
        public String toString() {
            return "User{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", fv=" + fv +
                    '}';
        }
    }
}

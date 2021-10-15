package cn._51doit.day02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 基于文件的Source
 *  默认情况下，Source读取完数据，程序就退出了，因为返回的是一个有限的数据流
 *  readTextFile底层调用的是readFile,读取数据的方式为FileProcessingMode.PROCESS_ONCE
 */
public class FileSource {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081); //指定本地web页面的服务的端口号
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //调用FileSource
        DataStreamSource<String> lines = env.readTextFile("/Users/xing/Desktop/a.txt");

        int parallelism = lines.getParallelism();

        System.out.println("readTextFile得到的DataStreamSource的并行度为："+ parallelism);

        lines.print();

        env.execute();

    }
}

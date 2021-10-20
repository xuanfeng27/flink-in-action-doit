package cn._51doit.day02;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * 基于文件的Source
 *
 *  调用readFile，指定FileProcessingMode.PROCESS_CONTINUOUSLY，一直会监听指定文件中的内容，这样得到的DataStream是一个无无限的数据流
 *  虽然会一直监听文件，但是文件发生变化，会将全部内容都度出来，会造成数据重复
 *
 */
public class FileSource2 {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081); //指定本地web页面的服务的端口号
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        String pathStr = "/Users/xing/Desktop/a.txt";
        TextInputFormat textInputFormat = new TextInputFormat(new Path(pathStr));

        DataStreamSource<String> lines = env.readFile(textInputFormat, pathStr, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000);

        int parallelism = lines.getParallelism();

        System.out.println("readTextFile得到的DataStreamSource的并行度为："+ parallelism);

        lines.print();

        env.execute();

    }
}

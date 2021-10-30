package cn._51doit.flink.sql;


import org.apache.flink.table.functions.TableFunction;

/**
 * UDTF  要继承 TableFunction
 */
public class Split extends TableFunction<String> {

    private String separator = ",";

    public Split() {}

    public Split(String separator) {
        this.separator = separator;
    }

    public void eval(String line) {
        for (String s : line.split(separator)) {
            collect(s);
        }
    }



}

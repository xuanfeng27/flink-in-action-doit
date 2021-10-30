package cn._51doit.flink.sql;

import org.apache.flink.table.functions.AggregateFunction;


public class MySumAggFunction extends AggregateFunction<Long, MySumAccumulator> {



    //获取累加后的结果
    @Override
    public Long getValue(MySumAccumulator accumulator) {
        return accumulator.count;
    }

    //创建初始值
    @Override
    public MySumAccumulator createAccumulator() {
        MySumAccumulator sumAccumulator = new MySumAccumulator();
        sumAccumulator.count = 0L;
        return sumAccumulator;
    }

    //定义一个accumulate的方法
    /**
     *
     * @param accumulator //累加的中间结果
     * @par
     * am in 当前数据的数据
     */
    public void accumulate(MySumAccumulator accumulator, Long in) {
       accumulator.count += in;
    }

}

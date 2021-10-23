package cn._51doit.day07;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.UUID;

/**
 * 再Source中定义OperatorState，是不跟key绑定再一起的
 * <p>
 * OperatorState 只有 一种形式 就是ListState
 * <p>
 * 如果想使用OperatorState必须实现一个CheckpointedFunction接口，重写两个方法initializeState和snapshotState
 * <p>
 * initializeState是用来初始化或恢复状态的
 * 每做一次checkpoint，会调用snapshotState方法
 * <p>
 * initializeState（一次） ->  open(一次) -> run(一次，run方法中有while循环) -> 每做一次checkpoint方实现了CheckpointedFunction的subtask会调用一次snapshotState
 */
public class MyAtLeastOnceFileSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {

    private boolean flag = true;
    private long offset = 0;
    //使用transient修饰的变量，不参与序列化和反序列号
    private String path;

    private transient ListState<Long> listState;

    public MyAtLeastOnceFileSource(String path) {
        this.path = path;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //在initializeState方法中初始化OperatorState和恢复OperatorState
        ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<>("offset-state", Long.class);
        listState = context.getOperatorStateStore().getListState(stateDescriptor);
        if(context.isRestored()) {
            for (Long offset : listState.get()) {
                //从状态中恢复偏移量，并且赋值给成员offset
                this.offset = offset;
            }
        }
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        //System.out.println("run method invoked###");
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

        RandomAccessFile raf = new RandomAccessFile(path + "/" + indexOfThisSubtask + ".txt", "r");
        //seek到指定的偏移量
        raf.seek(offset);
        while (flag) {
            String line = raf.readLine();
            if (line != null) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(indexOfThisSubtask + " : " + line);
                    //更新偏移量
                    offset = raf.getFilePointer();
                }
            } else {
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    /**
     * 每做一次快照就会调用一次snapshotState
     * 调用该方法，就会listState.add(offset)保存到StateBackend中
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //清除以前的状态
        listState.clear();
        //将最新的偏移量存储起来
        listState.add(offset);
        //listState.update(Collections.singletonList(offset));
    }


}

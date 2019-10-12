package com.yaxin.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean  implements Writable {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public FlowBean() {
    }

    public void set(long upFlow, long downFlow) {
        this.downFlow = downFlow;
        this.upFlow = upFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    /**
     * 序列化方法
     * @param out 框架给我们提供的数据出口
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        //通过out写给我们的框架
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);

        //Hadoop的序列化->只序列化数据,其他数据全丢失了,比如类的结构,我需要手动维护类的结构/类型
    }

    /**
     * 注意事项:序列化和反序列的顺序要一致,如果不一致会造成结果不正确
     * 如果类型不同,运行的时候会报错
     * /

     /**
     * 反序列化方法
     * @param in 框架给我们提供的数据来源
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        //通过in让框架把数据交还给我们
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }
}

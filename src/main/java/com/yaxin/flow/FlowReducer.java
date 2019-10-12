package com.yaxin.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean sumFlow = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //初始化两个累加值
        long sumUpFlow = 0;
        long sumDownFlow = 0;

        for(FlowBean value: values){
            sumUpFlow+=value.getUpFlow();
            sumDownFlow+=value.getDownFlow();
        }
        sumFlow.set(sumUpFlow,sumDownFlow);
        //把我们处理过后的数据再交给框架
        context.write(key,sumFlow);
    }
}

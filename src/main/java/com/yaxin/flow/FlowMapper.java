package com.yaxin.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text phone = new Text();
    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        //手机号是第一个元素(从零下标开始)
        phone.set(fields[1]);
        //获取上下行流量:因为每一个人不一定都有网址,不能正着数下标,要倒着数
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        //设置upFlow和downFlow
        flow.setUpFlow(upFlow);
        flow.setDownFlow(downFlow);
        //写回给框架处理逻辑
        context.write(phone,flow);
    }
}

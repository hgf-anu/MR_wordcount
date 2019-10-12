package com.yaxin.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length!=2){
            System.out.println("目录个数不正确");
            System.exit(2);
        }

        //1.获取Job实力
        Job job = Job.getInstance(new Configuration());

        //2.设置类路径
        job.setJarByClass(FlowDriver.class);

        //3.设置Mapper和Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4.设置输入/输出类型:这里就是我们告诉框架我们计算参数的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5.设置输入输出路径
        //注意导包是长包[新的].Hadoop是经历过一次大的更新,1.x=>2.x
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6.提交
        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);

    }
}

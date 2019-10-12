package com.yaxin.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {
    /**
     * 对任务进行相关设置(套路深,基本没变化)
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length!=2){
            System.out.println("参数个数不正确!");
            System.exit(1);
        }
        //1.获取一个Job实例,job是整个mapreduce的一个贯穿线,context=>Job
        Job job = Job.getInstance(new Configuration());

        //2.设置我们的累路径
        job.setJarByClass(WcDriver.class);

        //3.设置Mapper和Reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        //4.设置Mapper和Reducer输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //Reducer的输出就是最后的输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //5.设置输入输出数据源,以java -jar 输入路径 输出路径方式
        //FileInputFormat导包是选择长的包
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6.提交我们的job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}

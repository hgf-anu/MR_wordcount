package com.yaxin.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer处理的数据是来自经过Mapper处理过的数据
 * 第一个参数和第二个参数就是Mapper的输出的泛型类型
 */
public class WcReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable total = new IntWritable();
    private Text key = new Text();

    /**
     * mapreduce神奇的地方:第一个参数和第二个参数是一组k-v值,按key分组
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable value : values){
            sum += value.get();
        }
        total.set(sum);
        context.write(key,total);
    }
}

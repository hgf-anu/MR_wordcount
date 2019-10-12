package com.yaxin.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Map是映射的意思,不是地图.做数据的转换
 * 第一个参数LongWritable是offset,文件是以行的形式读进来的,offset描述的是当前字符距离起点的偏移量,注意每行结尾有个换行符,也是会被计算为一位
 * 第二个参数Text是Hadoop中String的封装类,这里是我们每行读的句子
 * 第三个参数和第四个参数是我们想要转换的数据格式
 */
public class WcMapper extends Mapper <LongWritable,Text,Text,IntWritable>{
    //快速重写方法:ctrl + o

    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    /**
     *
     * 框架里黑箱操作太多,我们只编写Mapper和Reducer,
     * 我们使用Context获取输入数据,map方法输出数据又会给context,让context和框架交互
     * @param key
     * @param value
     * @param context 上下文语境/任务
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.拿到这一行数据
        String line = value.toString();

        //2.按照空格切分数据
        String[] words = line.split(" ");

        /*
        //3.遍历数组,把单词变成(word,1)的形式交给框架
        for (String word : words){
            //这里输出的类型和父类的KEYIN,VLALUEOUT一致
            context.write(new Text(word),new IntWritable(1));
        }
        这种方式会生成(new)大量新的对象[每读一行数据就会new一个对象],导致垃圾回收,程序会越来越慢,这里仿照官方API
        */

        for (String word : words){
            this.word.set(word);
            context.write(this.word,one);
        }
    }

}

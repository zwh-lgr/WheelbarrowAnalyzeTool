package com.wheelanalyser;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.wheelanalyser.util.parser.TimeParser;
import com.wheelanalyser.writable.ReduceResultPair;
import com.wheelanalyser.writable.TextPair;
import com.google.common.collect.Iterables;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IntervalAmountRCalculater {
    public static class UserDateMapper extends Mapper<Object, Text, TextPair, Text> {
        private TextPair key = new TextPair();
        private Text commentTime = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString().replace("\"", "");

            // 回避csv格式文件首行，具体需要根据csv文件的格式来定
            if (!text.contains("time,room_id,sender_name")) {
                String item[] = text.split(",");
               
                this.key.setFirst(new Text(item[1]));
                this.key.setSecond(new Text(item[2]));
                this.commentTime.set(item[0]);
                context.write(this.key, this.commentTime);
            }
        }
    }

    public static class UserDateReducer extends Reducer<TextPair, Text, TextPair, ReduceResultPair> {
        private DoubleWritable coeffient = new DoubleWritable();
        private ReduceResultPair result = new ReduceResultPair();
        @Override
        public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Text> valueList = new ArrayList<Text>();
            int sum = 0;
            for (Text value : values) {
                Text valueClone = new Text(value);
                valueList.add(valueClone);
                sum++;
            }

            if (sum > 2) {
                // 仅处理发言数大于2的用户
                int count = 0; // 发言数
                long interval = 0; // 本次发言距第一次发言的间隔
                Text time = new Text(); // 发言时间
                // 将发言间隔作为x，发言数作为y
                double[] xData = new double[sum];
                double[] yData = new double[sum];

                // 对发言时间进行排序
                Collections.sort(valueList);
                // 以排序后的第一个时间作为计算原点
                LocalDateTime oX = TimeParser.parseToLocalDateTime(Iterables.getFirst(valueList, null).toString());
                for (Text value : valueList) {
                    time = value;
                    interval = Math
                            .abs(Duration.between(oX, TimeParser.parseToLocalDateTime(time.toString())).getSeconds());
                    xData[count] = (double) interval;
                    yData[count] = (double) count + 1;
                    count++;
                }

                // 计算皮尔逊相关系数，由于间隔与发言数成正比例关系，独轮车用户的相关系数R将会接近1
                this.coeffient.set(new PearsonsCorrelation().correlation(xData, yData));
                this.result.setCoeffient(coeffient);
                this.result.setSum(new IntWritable(sum));
                context.write(key, result);
            } 
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "IntervalAmountRCalculate");
        job.setJarByClass(IntervalAmountRCalculater.class);
        job.setMapperClass(UserDateMapper.class);
        job.setReducerClass(UserDateReducer.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(ReduceResultPair.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

package com.wheelanalyser;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.wheelanalyser.util.parser.TimeParser;
import com.google.common.collect.Iterables;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IntervalAmountRCalculater {
    public static class UserDateMapper extends Mapper<Object, Text, Text, Text> {
        private Text userName = new Text();
        private Text commentTime = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString().replace("\"", "");

            // 回避csv格式文件首行，具体需要根据csv文件的格式来定
            if (!text.contains("id,time")) {
                // text.replaceAll("\"", "");
                String item[] = text.split(",");
               
                this.userName.set(item[2]);
                this.commentTime.set(item[1]);
                context.write(this.userName, this.commentTime);
            }
        }
    }

    public static class UserDateReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Text> valueList = new ArrayList<Text>();
            for (Text value : values) {
                Text valueClone = new Text(value);
                valueList.add(valueClone);
            }

            int valueSize = Iterables.size(valueList);

            if (valueSize > 1) {
                // 仅处理发言数大于1的用户
                int count = 0; // 发言数
                long interval = 0; // 本次发言距第一次发言的间隔
                Text time = new Text(); // 发言时间
                // 将发言间隔作为x，发言数作为y
                double[] xData = new double[valueSize];
                double[] yData = new double[valueSize];

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
                }

                // 计算皮尔逊相关系数，由于间隔与发言数成正比例关系，独轮车用户的相关系数R将会接近1
                this.result.set(new PearsonsCorrelation().correlation(xData, yData));
                context.write(key, result);
            } else {
                // 将发言次数仅为一次的用户的结果设为-1
                this.result.set(-1.0);
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
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

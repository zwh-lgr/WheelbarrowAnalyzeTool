package com.wheelanalyser;

import java.io.IOException;
import java.io.StringReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.wheelanalyser.bean.Barrage;
import com.wheelanalyser.util.file.FileUtil;
import com.wheelanalyser.util.parser.TimeParser;
import com.google.common.collect.Iterables;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.bean.CsvToBeanBuilder;

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
            String text = value.toString();
            if(!text.contains("\"id\",\"time\"")){
                StringReader stringReader = new StringReader(text);
                CSVReaderBuilder builder = new CSVReaderBuilder(stringReader);
                CSVReader csvReader = builder.build();
                List<Barrage> barrages = new CsvToBeanBuilder<Barrage>(csvReader)
                    .withType(Barrage.class).build().parse();
                for(Barrage barrage:barrages){
                    this.userName.set(barrage.getId());
                    this.commentTime.set(barrage.getTime());
                    context.write(this.userName, this.commentTime);
                }
            }
        }
    }

    public static class UserDateReducer extends Reducer<Text, Text, Text, DoubleWritable>{
        private DoubleWritable result = new DoubleWritable();
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Text> valueList = new ArrayList<Text>();
            for(Text value:values){
                Text valueClone = new Text(value);
                valueList.add(valueClone);
            }
            
            int valueSize = Iterables.size(valueList);
            
            if(valueSize > 1){
                int count = 0;
                long interval = 0;
                Text time = new Text();
                double[] xData = new double[valueSize];
                double[] yData = new double[valueSize];
                
                Collections.sort(valueList);
                LocalDateTime oX = TimeParser.parseToLocalDateTime(Iterables.getFirst(valueList, null).toString());
                for(Text value:valueList){
                    time = value;
                    interval = Math.abs(Duration.between(oX, TimeParser.parseToLocalDateTime(time.toString())).toSeconds());
                    xData[count] = (double)interval;
                    yData[count] = (double)count+1;
                }
               
                this.result.set(new PearsonsCorrelation().correlation(xData,yData));
                context.write(key, result);                   
            }else{
                this.result.set(-1.0);
                context.write(key, result);
            }           
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileUtil.deleteDir("output");
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

        System.exit(job.waitForCompletion(true)?0:1);
    }
}

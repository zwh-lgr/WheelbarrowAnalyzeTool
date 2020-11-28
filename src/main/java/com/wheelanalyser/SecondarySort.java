package com.wheelanalyser;

import java.io.IOException;

import com.wheelanalyser.writable.RecordPair;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySort {
    public static class SortMapper extends Mapper<Object, Text, RecordPair, NullWritable> {
        private RecordPair key = new RecordPair();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String items[] = value.toString().split("\t");
            this.key.setRoomId(new Text(items[0]));
            this.key.setUserId(new Text(items[1]));
            this.key.setSum(new IntWritable(Integer.parseInt(items[3])));
            this.key.setCoeffient(new DoubleWritable(Double.parseDouble(items[2])));
            context.write(this.key, NullWritable.get());
        }
    }

    public static class SortReducer extends Reducer<RecordPair, NullWritable, RecordPair, NullWritable> {
        @Override
        public void reduce(RecordPair key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
}

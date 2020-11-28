package com.wheelanalyser.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ReduceResultPair implements WritableComparable<ReduceResultPair> {
    /**
     * 第一个字段接收弹幕统计，第二个字段接收皮尔逊相关系数
     */
    public IntWritable sum;
    public DoubleWritable coeffient;

    public ReduceResultPair() {
        this.sum = new IntWritable();
        this.coeffient = new DoubleWritable();
    }

    public ReduceResultPair(IntWritable sum, DoubleWritable coeffient) {
        this.sum = sum;
        this.coeffient = coeffient;
    }

    public ReduceResultPair(int sum, double coeffient) {
        this.sum = new IntWritable(sum);
        this.coeffient = new DoubleWritable(coeffient);
    }

    public IntWritable getSum() {
        return sum;
    }

    public void setSum(IntWritable sum) {
        this.sum = sum;
    }

    public DoubleWritable getCoeffient() {
        return coeffient;
    }

    public void setCoeffient(DoubleWritable coeffient) {
        this.coeffient = coeffient;
    }

    @Override
    public String toString() {
        return coeffient + "\t" + sum;
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        sum.write(arg0);
        coeffient.write(arg0);
    }

    @Override
    public int compareTo(ReduceResultPair o) {
        int cmp = sum.compareTo(o.sum);
        if (cmp != 0) {
            return cmp;
        }
        return coeffient.compareTo(o.coeffient);
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        sum.readFields(arg0);
        coeffient.readFields(arg0);
    }

}

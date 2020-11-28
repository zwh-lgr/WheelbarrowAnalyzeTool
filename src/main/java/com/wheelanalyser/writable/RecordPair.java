package com.wheelanalyser.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class RecordPair implements WritableComparable<RecordPair>{

    public Text roomId;
    public Text userId;
    public IntWritable sum;
    public DoubleWritable coeffient;

    public RecordPair() {
        this.roomId = new Text();
        this.userId = new Text();
        this.sum = new IntWritable();
        this.coeffient = new DoubleWritable();
    }

    public RecordPair(Text first, Text second, IntWritable sum, DoubleWritable coeffient) {
        this.roomId = first;
        this.userId = second;
        this.sum = sum;
        this.coeffient = coeffient;
    }

    public RecordPair(String first, String second, int sum, double coeffient) {
        this.roomId = new Text(first);
        this.userId = new Text(second);
        this.sum = new IntWritable(sum);
        this.coeffient = new DoubleWritable(coeffient);
    }
 
    public Text getRoomId() {
        return roomId;
    }

    public void setRoomId(Text roomId) {
        this.roomId = roomId;
    }

    public Text getUserId() {
        return userId;
    }

    public void setUserId(Text userId) {
        this.userId = userId;
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
        return roomId + "\t" + userId + "\t" + sum + "\t" + coeffient;
    }

    @Override
    public int hashCode() {
        return roomId.hashCode() * 163 + userId.hashCode();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        roomId.write(arg0);
        userId.write(arg0);
        sum.write(arg0);
        coeffient.write(arg0);
    }

    @Override
    public int compareTo(RecordPair o) {
        int cmp1 = roomId.compareTo(o.roomId);
        int cmp2 = sum.compareTo(o.sum);
        if (cmp1 != 0) {
            return cmp1;
        } else if(cmp2 != 0){
            return -cmp2;
        }
        return -coeffient.compareTo(o.coeffient);
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        roomId.readFields(arg0);
        userId.readFields(arg0);
        sum.readFields(arg0);
        coeffient.readFields(arg0);
    }

}

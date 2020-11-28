package com.wheelanalyser.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextPair implements WritableComparable<TextPair> {
    /*
     * 用以存储直播间id和弹幕发送者的id
     */
    public Text first;
    public Text second;

    public TextPair() {
        this.first = new Text();
        this.second = new Text();
    }

    public TextPair(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public TextPair(String first, String second) {
        this.first = new Text(first);
        this.second = new Text(second);
    }

    public Text getFirst() {
        return first;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public Text getSecond() {
        return second;
    }

    public void setSecond(Text second) {
        this.second = second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public int compareTo(TextPair tp) {
        int cmp = first.compareTo(tp.first);
        if(cmp!=0){
            return cmp;
        }
        return second.compareTo(tp.second);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof TextPair){
            TextPair tp = (TextPair) obj;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }
    
    public static class NatureKeyComparator extends WritableComparator{
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
        public NatureKeyComparator(){
            super(TextPair.class);
        }

        
    }
}

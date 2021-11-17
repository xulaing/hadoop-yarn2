package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer extends Reducer<FloatWritable, Text, Text, FloatWritable>  {
    public void reduce(FloatWritable height, Iterable<Text> kinds, Context context) throws IOException, InterruptedException{
        for(Text kind : kinds){
            context.write(kind, height); // Write the kind and its height
        }
    }

}
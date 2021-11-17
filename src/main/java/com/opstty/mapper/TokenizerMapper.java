package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable>{
    private int line = 0;

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if (line != 0){ // Skip Header
            try{
                String[] fields = value.toString().split(";");
                Text kind = new Text(fields[11] + " - " + fields[2] + " : "); // Get the kind
                Float height = Float.parseFloat(fields[6]); // Get its height
                context.write(new FloatWritable(height), kind); // Write both of them in the context
            }catch(NumberFormatException ex){}
        }
        line++;
    }
}

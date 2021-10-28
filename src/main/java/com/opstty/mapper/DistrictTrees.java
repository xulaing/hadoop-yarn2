package com.opstty.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class DistrictTrees extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {
    private final static IntWritable zero = new IntWritable(0);

    public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] Data = valueString.split(";");
        output.collect(new Text(Data[1]), zero);
    }
}

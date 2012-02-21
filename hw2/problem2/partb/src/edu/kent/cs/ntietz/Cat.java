package edu.kent.cs.ntietz;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Cat
{

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException
        {
            output.collect(value, new IntWritable(1));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text>
    {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException
        {
            output.collect(key, new Text(""));
        }
    }

    public static void main(String... args)
    throws IOException
    {   
        JobConf conf = new JobConf(Pairs.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(SentenceInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}


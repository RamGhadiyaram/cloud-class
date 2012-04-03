package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Check
{
    public static class CheckMap
    extends MapReduceBase
    implements Mapper<LongWritable, Node, LongWritable, Node>
    {
        public void map( LongWritable key
                       , Node value
                       , OutputCollector<LongWritable, Node> output
                       , Reporter reporter
                       )
        throws IOException
        {
            output.collect(key, value);
        }
    }

    public static class CheckReduce
    extends MapReduceBase
    implements Reducer<LongWritable, Node, Text, Text>
    {
        public void reduce( LongWritable key
                          , Iterator<Node> values
                          , OutputCollector<Text, Text> output
                          , Reporter reporter
                          )
        throws IOException
        {
            while (values.hasNext())
            {
                Node value = values.next();
                output.collect(new Text(key.toString()), new Text(value.toString()));
            }
        }
    }

    public static void main(String... args)
    throws IOException
    {
        JobConf conf = new JobConf(Check.class);
        conf.setJobName("check");

        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(Node.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(CheckMap.class);
        conf.setReducerClass(CheckReduce.class);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        conf.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        JobClient.runJob(conf);
    }
}


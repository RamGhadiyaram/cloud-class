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

public class ParagraphPairs
{
    public static class Reduce extends MapReduceBase implements Reducer<KeyPair, IntWritable, Text, Text>
    {
        private int totalCounts = 0;

        public void reduce(KeyPair key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException
        {
            if (key.marginal)
            {
                totalCounts = 0;
                while (values.hasNext())
                {
                    totalCounts += values.next().get();
                }
            }
            else
            {
                int sum = 0;
                while (values.hasNext())
                {
                    sum += values.next().get();
                }
                double probability = ((double)sum) / totalCounts;

                String outputPair = key.left + "," + key.right;
                String outputText = sum + "," + probability;
                output.collect(new Text(outputPair), new Text(outputText));
            }
        }
    }

    public static void main(String... args)
    throws IOException
    {
        JobConf conf = new JobConf(ParagraphPairs.class);
        conf.setJobName("partc-pairs");

        conf.setOutputKeyClass(KeyPair.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(PairMapper.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(ParagraphInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumReduceTasks(8);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        FileOutputFormat.setCompressOutput(conf, false);

        JobClient.runJob(conf);
    }
}


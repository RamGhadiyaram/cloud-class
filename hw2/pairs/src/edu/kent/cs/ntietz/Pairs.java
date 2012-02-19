package edu.kent.cs.ntietz;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Pairs
{

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            List<String> words = new ArrayList<String>();

            while (tokenizer.hasMoreTokens())
            {
                words.add(tokenizer.nextToken());
            }

            for (int i = 0; i < words.size(); ++i)
            {
                for (int j = 0; j < words.size(); ++j)
                {
                    if (i != j)
                    {
                        String pair = String.valueOf(words.get(i)) + " " + String.valueOf(words.get(j));
                        output.collect(new Text(pair), new IntWritable(1));
                    }
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException
        {
            int sum = 0;
            while (values.hasNext())
            {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
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

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}


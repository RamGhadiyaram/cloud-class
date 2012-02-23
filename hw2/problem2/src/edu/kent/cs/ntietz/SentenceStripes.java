package edu.kent.cs.ntietz;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SentenceStripes
{

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, MapWritable>
    {
        public void map(LongWritable key, Text value, OutputCollector<Text, MapWritable > output, Reporter reporter)
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
                MapWritable stripe = new MapWritable();

                Text current = new Text(words.get(i));

                for (int j = 0; j < words.size(); ++j)
                {
                    if (i != j)
                    {
                        Text other = new Text(words.get(j));
                        if (!stripe.containsKey(other))
                        {
                            stripe.put(other, new IntWritable(1));
                        }
                        else
                        {
                            stripe.put(other, new IntWritable(((IntWritable)stripe.get(other)).get()+1));
                        }
                    }
                }

                output.collect(current, stripe);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, MapWritable, Text, Text>
    {
        public void reduce(Text key, Iterator<MapWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException
        {
            MapWritable result = new MapWritable();
            int totalCount = 0;

            while (values.hasNext())
            {
                MapWritable nextStripe = values.next();

                for (Writable wCurrent : nextStripe.keySet())
                {
                    Text current = (Text)wCurrent;

                    if (!result.containsKey(current))
                    {
                        result.put(current, nextStripe.get(current));
                    }
                    else
                    {
                        result.put(current, new IntWritable(((IntWritable)result.get(wCurrent)).get() + ((IntWritable)nextStripe.get(wCurrent)).get()));
                    }

                    totalCount += ((IntWritable)nextStripe.get(wCurrent)).get();
                }
            }

            String outputText = new String();
            for (Writable wEach : result.keySet())
            {
                Text each = (Text)wEach;
                Integer times = ((IntWritable)result.get(each)).get();
                double probability = ((double)times) / totalCount;

                outputText += each.toString() + ":" + String.valueOf(times) + "," + probability + "        ";
            }
            output.collect(key, new Text(outputText));
        }
    }

    public static void main(String... args)
    throws IOException
    {
        JobConf conf = new JobConf(SentenceStripes.class);
        conf.setJobName("partb-stripes");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(MapWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(SentenceInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumReduceTasks(8);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        FileOutputFormat.setCompressOutput(conf, false);

        JobClient.runJob(conf);
    }
}


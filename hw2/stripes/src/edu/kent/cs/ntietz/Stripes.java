package edu.kent.cs.ntietz;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Stripes
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
                        if (stripe.get(current) == null)
                        {
                            stripe.put(current, new IntWritable(1));
                        }
                        else
                        {
                            stripe.put(current, new IntWritable(((IntWritable)stripe.get(current)).get()+1));
                        }
                    }
                }

                output.collect(new Text(words.get(i)), stripe);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, MapWritable, Text, MapWritable>
    {
        public void reduce(Text key, Iterator<MapWritable> values, OutputCollector<Text, MapWritable> output, Reporter reporter)
        throws IOException
        {
            MapWritable result = new MapWritable();

            while (values.hasNext())
            {
                MapWritable nextStripe = values.next();

                for (Writable wCurrent : nextStripe.keySet())
                {
                    Text current = (Text)wCurrent;

                    if (!result.containsKey(current))
                    {
                        result.put(key, nextStripe.get(current));
                    }
                    else
                    {
                        result.put(key, new IntWritable(((IntWritable)result.get(wCurrent)).get() + ((IntWritable)nextStripe.get(wCurrent)).get()));
                    }
                }
            }

            output.collect(key, result);
        }
    }

    public static void main(String... args)
    throws IOException
    {
        JobConf conf = new JobConf(Stripes.class);
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


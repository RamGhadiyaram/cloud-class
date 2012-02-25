package edu.kent.cs.ntietz;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class StripeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, MapWritable>
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



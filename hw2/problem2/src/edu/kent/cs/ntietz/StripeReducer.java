package edu.kent.cs.ntietz;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class StripeReducer extends MapReduceBase implements Reducer<Text, MapWritable, Text, Text>
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


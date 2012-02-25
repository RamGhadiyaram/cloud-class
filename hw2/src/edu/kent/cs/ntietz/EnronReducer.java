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

public class EnronReducer
extends MapReduceBase
implements Reducer<Text, MapWritable, Text, Text>
{
    public void reduce( Text key
                      , Iterator<MapWritable> values
                      , OutputCollector<Text, Text> output
                      , Reporter reporter
                      )
    throws IOException
    {
        Map<String, Integer> result = new HashMap<String, Integer>();
        int totalCount = 0;

        while (values.hasNext())
        {
            MapWritable stripe = values.next();

            for (Writable w : stripe.keySet())
            {
                String word = ((Text)w).toString();
                Integer count = ((IntWritable)stripe.get(w)).get();

                if (!result.containsKey(word))
                {
                    result.put(word, count);
                }
                else
                {
                    result.put(word, result.get(word) + count);
                }

                totalCount += count;
            }
        }

        String outputText = new String();

        for (String word : result.keySet())
        {
            int count = result.get(word);
            double probability = ((double)count) / totalCount;
            outputText += word + ":";
            outputText += count + ",";
            outputText += probability + ";  ";
        }

        output.collect(key, new Text(outputText));
    }
}


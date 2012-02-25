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

public class EnronMapper
extends MapReduceBase
implements Mapper<LongWritable, Text, Text, MapWritable>
{
    public void map( LongWritable key
                   , Text value
                   , OutputCollector<Text, MapWritable> output
                   , Reporter reporter
                   )
    throws IOException
    {
        Map<String,MapWritable> counts = new HashMap<String,MapWritable>();

        String email = value.toString();
        List<String> lines = Arrays.asList(email.split("\\n"));

        // strip out the message header - remove it until we get to an empty line
        while (!lines.get(0).equals(""))
        {
            lines.remove(0);
        }

        String body = "";
        for (String each : lines)
        {
            body += each + "\n";
        }

        // split into the words
        StringTokenizer tokenizer = new StringTokenizer(body);
        List<String> words = new ArrayList<String>();

        while (tokenizer.hasMoreTokens())
        {
            words.add(tokenizer.nextToken());
        }

        // find the co-occurrences for the entire email, use stripes
        for (String each : words)
        {
            MapWritable stripe = counts.get(each);
            if (stripe == null)
            {
                stripe = new MapWritable();
            }

            for (String other : words)
            {
                IntWritable current = (IntWritable)stripe.get(other);
                if (current == null)
                {
                    current = new IntWritable(0);
                }
                stripe.put(new Text(other), new IntWritable(current.get()+1));
            }

            counts.put(each, stripe);
        }

        for (String word : counts.keySet())
        {
            output.collect(new Text(word), counts.get(word));
        }
    }
}


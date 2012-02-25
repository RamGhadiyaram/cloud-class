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

public class PairMapper extends MapReduceBase implements Mapper<LongWritable, Text, KeyPair, IntWritable>
{
    public void map(LongWritable key, Text value, OutputCollector<KeyPair, IntWritable> output, Reporter reporter)
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
            int count = 0;
            for (int j = 0; j < words.size(); ++j)
            {
                if (i != j)
                {
                    KeyPair pair = new KeyPair();
                    pair.left = words.get(i);
                    pair.right = words.get(j);
                    output.collect(pair, new IntWritable(1));
                    ++count;
                }
            }
            KeyPair marginal = new KeyPair();
            marginal.left = words.get(i);
            marginal.right = "";
            marginal.marginal = true;
            output.collect(marginal, new IntWritable(count));
        }
    }
}


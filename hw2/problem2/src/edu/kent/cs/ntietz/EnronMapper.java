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
implements Mapper<LongWritable, Text, KeyPair, IntWritable>
{
    public void map(LongWritable key, Text value, OutputCollector<KeyPair, IntWritable> output, Reporter reporter)
    {
        String email = value.toString();
        List<String> lines = Arrays.asList(email.split("\\n"));

        // strip out the message header - remove it until we get to an empty line
        while (!lines.get(0).equals(""))
        {
            lines.remove(0);
        }

        // find the co-occurrences for the entire email, use stripes

        // emit all the stripes
    }
}


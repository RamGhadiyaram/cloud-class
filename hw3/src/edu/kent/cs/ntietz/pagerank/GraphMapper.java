package edu.kent.cs.ntietz.pagerank;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class GraphMapper
extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>
{
    public void map( LongWritable key
                   , Text line
                   , OutputCollector<Text, Text> output
                   , Reporter reporter
                   )
    throws IOException
    {
        String[] values = line.toString().split(",");

        String start = values[0];
        String destination = values[1];

        output.collect(new Text(start), new Text(destination));
    }
}


package edu.kent.cs.ntietz.pagerank;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class GraphMapper
extends MapReduceBase
implements Mapper<LongWritable, Text, LongWritable, Text>
{
    public void map( LongWritable key
                   , Text line
                   , OutputCollector<LongWritable, Text> output
                   , Reporter reporter
                   )
    throws IOException
    {
        String[] values = line.toString().split(",");

        String start = values[0];
        String destination = values[1];

        reporter.incrCounter("NUMBER","EDGES",1);

        output.collect(new LongWritable(Long.valueOf(start)), new Text(destination));
    }
}


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
        // for our data set, this results in a size-3 array:
        //  values => { lineNumber, node following, node being followed }
        String[] values = line.toString().split("[\\s,]+");

        String start = values[1];
        String destination = values[2];

        output.collect(new Text(start), new Text(destination));
    }
}


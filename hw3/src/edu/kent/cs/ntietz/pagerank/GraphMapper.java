package edu.kent.cs.ntietz.pagerank;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class GraphMapper
extends MapReduceBase
implements Mapper<LongWritable, Text, Node, Node>
{
    public static final int numberOfNodes = 1131681;
    public static final double defaultWeight = 1.0 / numberOfNodes;

    public void map( LongWritable key
                   , Text line
                   , OutputCollector<Node, Node> output
                   , Reporter reporter
                   )
    throws IOException
    {
        // for our data set, this results in a size-3 array:
        //  values => { lineNumber, node following, node being followed }
        String[] values = line.toString().split("[\\s,]+");

        Node start = new Node();
        start.name = values[1];
        start.score = defaultWeight;

        Node destination = new Node();
        destination.name = values[2];
        destination.score = defaultWeight;

        output.collect(start, destination);
    }
}


package edu.kent.cs.ntietz.pagerank;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class GraphMapper
extends MapReduceBase
implements Mapper<LongWritable, Text, Node, AdjacencyList>
{
    public void map( LongWritable key
                   , Text line
                   , OutputCollector<Node, AdjacencyList> output
                   , Reporter reporter
                   )
    throws IOException
    {

    }
}


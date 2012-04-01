package edu.kent.cs.ntietz.pagerank;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageRankMapper
extends MapReduceBase
implements Mapper<Node, AdjacencyList, Node, Node>
{
    public void map( Node key
                   , AdjacencyList value
                   , OutputCollector<Node, Node> output
                   , Reporter reporter
                   )
    throws IOException
    {

    }
}


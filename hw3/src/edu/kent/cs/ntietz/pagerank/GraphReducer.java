package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class GraphReducer
extends MapReduceBase
implements Reducer<Node, Node, Node, AdjacencyList>
{
    public void reduce( Node key
                      , Iterator<Node> values
                      , OutputCollector<Node, AdjacencyList> output
                      , Reporter reporter
                      )
    throws IOException
    {
        
    }
}


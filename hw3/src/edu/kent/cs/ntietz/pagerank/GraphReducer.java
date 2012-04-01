package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class GraphReducer
extends MapReduceBase
implements Reducer<Node, AdjacencyList, Node, AdjacencyList>
{
    public void reduce( Node key
                      , Iterator<AdjacencyList> values
                      , OutputCollector<Node, AdjacencyList> output
                      , Reporter reporter
                      )
    throws IOException
    {

    }
}


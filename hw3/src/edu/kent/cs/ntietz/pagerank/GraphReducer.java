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
        List<String> neighbors = new ArrayList<String>();

        while (values.hasNext())
        {
            neighbors.add(values.next().name);
        }

        AdjacencyList list = new AdjacencyList();
        list.node = key.name;
        list.neighbors = neighbors;

        output.collect(key, list);
    }
}


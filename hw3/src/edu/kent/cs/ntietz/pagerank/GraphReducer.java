package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class GraphReducer
extends MapReduceBase
implements Reducer<Text, Text, Text, Node>
{
    public void reduce( Text key
                      , Iterator<Text> values
                      , OutputCollector<Text, Node> output
                      , Reporter reporter
                      )
    throws IOException
    {
        Node node = new Node();
        node.name = key.toString();
        node.score = Node.defaultWeight;
        
        List<String> neighbors = new ArrayList<String>();

        while (values.hasNext())
        {
            neighbors.add(values.next().toString());
        }

        AdjacencyList list = new AdjacencyList();
        list.members = neighbors;

        node.neighbors = list;

        output.collect(key, node);
    }
}


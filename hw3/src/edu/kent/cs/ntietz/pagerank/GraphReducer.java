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
        node.name = key;
        node.score = Node.defaultWeight;
        



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


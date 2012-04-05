package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class GraphReducer
extends MapReduceBase
implements Reducer<LongWritable, Text, LongWritable, Node>
{
    public void reduce( LongWritable key
                      , Iterator<Text> values
                      , OutputCollector<LongWritable, Node> output
                      , Reporter reporter
                      )
    throws IOException
    {
        Node node = new Node();
        node.name = key.toString();
        node.score = Node.DEFAULT;
        
        List<String> neighbors = new ArrayList<String>();

        while (values.hasNext())
        {
            String next = values.next().toString();

            if (!next.equals(""))
            {
                neighbors.add(next);
            }
        }

        AdjacencyList list = new AdjacencyList();
        list.members = neighbors;
        node.neighbors = list;

        reporter.incrCounter("NUMBER", "NODES", 1);

        if (neighbors.size() == 0)
        {
            reporter.incrCounter("NUMBER", "DANGLING", 1);
        }

        output.collect(key, node);
    }
}


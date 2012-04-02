package edu.kent.cs.ntietz.pagerank;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageRankMapper
extends MapReduceBase
implements Mapper<Text, Node, Text, Contribution>
{
    public void map( Node key
                   , AdjacencyList value
                   , OutputCollector<Node, Node> output
                   , Reporter reporter
                   )
    throws IOException
    {
        int numberOfNeighbors = value.neighbors.size();
        double contribution = key.score / numberOfNeighbors;

        key.previous = true;
        output.collect(key, key);

        Node contributor = new Node();
        contributor.name = key.name;
        contributor.score = contribution;

        for (String each : value.neighbors)
        {
            Node neighbor = new Node();
            neighbor.name = each;

            output.collect(neighbor, contributor);
        }
    }
}


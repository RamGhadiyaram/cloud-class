package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageRankReducer
extends MapReduceBase
implements Reducer<Text, Contribution, Text, Node>
{
    private double score = 0.0;

    public void reduce( Node key
                      , Iterator<Node> values
                      , OutputCollector<Node, AdjacencyList> output
                      , Reporter reporter
                      )
    throws IOException
    {
        if (key.previous)
        {
            score = key.score;
        }
        else
        {
            while (values.hasNext())
            {
                Node contribution = values.next();

                score += contribution.score;
            }
        }
    }
}


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

    public void reduce( Text key
                      , Iterator<Contribution> values
                      , OutputCollector<Text, Node> output
                      , Reporter reporter
                      )
    throws IOException
    {
        Node node = new Node();
        double score = 0.0;

        while (values.hasNext())
        {
            Contribution contribution = values.next();

            if (!contribution.isScore)
            {
                node = contribution.node;
            }
            else
            {
                score += contribution.score;
            }
        }

        // TODO add random surfing adjustment

        output.collect(key, node);
    }
}


package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageRankReducer
extends MapReduceBase
implements Reducer<LongWritable, Contribution, LongWritable, Node>
{
    private double score = 0.0;

    public void reduce( LongWritable key
                      , Iterator<Contribution> values
                      , OutputCollector<LongWritable, Node> output
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

        node.score = score;

        // TODO add random surfing adjustment

        output.collect(key, node);
    }
}


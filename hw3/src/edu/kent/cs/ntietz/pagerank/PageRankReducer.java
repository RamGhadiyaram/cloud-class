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
    private long numberOfNodes = 1;

    public void configure(JobConf conf)
    {
        numberOfNodes = Long.valueOf(conf.get("numberOfNodes"));
    }

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
                // if score is negative, it was Node.DEFAULT, so we use 1.0 / numberOfNodes instead
                if (contribution.score < 0)
                {
                    contribution.score = (contribution.score * -1.0 / numberOfNodes);
                }
                score += contribution.score;
            }
        }

        node.score = score;

        // TODO add random surfing adjustment

        output.collect(key, node);
    }
}


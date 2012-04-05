package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageRankReducer
extends MapReduceBase
implements Reducer<LongWritable, Contribution, LongWritable, Node>
{
    private long numberOfNodes = 1;
    private double alpha = 0.0;
    private double extraWeight = 0.0;

    public void configure(JobConf conf)
    {
        numberOfNodes = Long.valueOf(conf.get("numberOfNodes"));
        alpha = Double.valueOf(conf.get("alpha"));
        extraWeight = Double.valueOf(conf.get("extraWeight"));
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
                // if score is negative, it was Node.DEFAULT/numberOfNeighbors, so we scale it to (1/numberOfNodes) / numberOfNeighbors
                if (contribution.score < 0)
                {
                    contribution.score = (contribution.score / (numberOfNodes * Node.DEFAULT));
                }
                score += contribution.score;
            }
        }
        
        score += extraWeight;

        if (node.score < 0)
        {
            node.score = 1.0 / numberOfNodes;
        }

        score = alpha/numberOfNodes + (1-alpha) * score;

        double difference = Math.abs(score - node.score);

        long danglingUpdate = (long) (node.score * (numberOfNodes * Constants.inflationFactor));
        long convergenceUpdate = (long) (difference * (numberOfNodes * Constants.inflationFactor));

        reporter.incrCounter("WEIGHT", "CONVERGENCE", convergenceUpdate);

        if (node.isDangling())
        {
            reporter.incrCounter("WEIGHT", "DANGLING", danglingUpdate);
        }

        reporter.incrCounter("WEIGHT", "TOTAL", (long) ((score  * (numberOfNodes * Constants.inflationFactor))));

        node.score = score;

        output.collect(key, node);
    }
}


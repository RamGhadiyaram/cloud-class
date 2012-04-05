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
                // if score is negative, it was Node.DEFAULT, so we use 1.0 / numberOfNodes instead
                if (contribution.score < 0)
                {
                    contribution.score = (contribution.score * -1.0 / numberOfNodes);
                }
                score += contribution.score;
            }
        }

        score = alpha + (1-alpha) * score;

        double difference = Math.abs(score - node.score);

        long danglingUpdate = (long) (node.score * numberOfNodes * Constants.inflationFactor);
        long convergenceUpdate = (long) (difference * numberOfNodes * Constants.inflationFactor);

        reporter.incrCounter("WEIGHT", "CONVERGENCE", convergenceUpdate);
        reporter.incrCounter("WEIGHT", "DANGLING", danglingUpdate);

        // TODO increment counter here for how different the old score and the new score are, so we can measure convergence
        // TODO increment counter here for dangliing nodes

        node.score = score;

        output.collect(key, node);
    }
}


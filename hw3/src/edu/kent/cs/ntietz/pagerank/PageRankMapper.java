package edu.kent.cs.ntietz.pagerank;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageRankMapper
extends MapReduceBase
implements Mapper<LongWritable, Node, LongWritable, Contribution>
{
    public void map( LongWritable key
                   , Node value
                   , OutputCollector<LongWritable, Contribution> output
                   , Reporter reporter
                   )
    throws IOException
    {
        Contribution current = new Contribution();
        current.name = key.toString();
        current.isScore = false;
        current.node = value;

        output.collect(key, current);

        int numberOfNeighbors = value.neighbors.members.size();

        if (numberOfNeighbors > 0)
        {
            double contributionScore = value.score / numberOfNeighbors;

            Contribution contribution = new Contribution();
            contribution.score = contributionScore;

            reporter.incrCounter("NUMBER", "NODES", 1);

            for (String each : value.neighbors.members)
            {
                contribution.name = each;
                output.collect(new LongWritable(Long.valueOf(each)), contribution);
            }
        }
    }
}


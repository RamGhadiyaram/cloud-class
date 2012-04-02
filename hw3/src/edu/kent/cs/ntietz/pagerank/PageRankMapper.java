package edu.kent.cs.ntietz.pagerank;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageRankMapper
extends MapReduceBase
implements Mapper<Text, Node, Text, Contribution>
{
    public void map( Text key
                   , Node value
                   , OutputCollector<Text, Contribution> output
                   , Reporter reporter
                   )
    throws IOException
    {
        int numberOfNeighbors = value.neighbors.members.size();
        double contributionScore = value.score / numberOfNeighbors;

        Contribution current = new Contribution();
        current.name = key.toString();
        current.isScore = false;
        current.node = value;

        output.collect(key, current);

        Contribution contribution = new Contribution();
        contribution.score = contributionScore;

        for (String each : value.neighbors.members)
        {
            contribution.name = each;
            output.collect(new Text(each), contribution);
        }
    }
}


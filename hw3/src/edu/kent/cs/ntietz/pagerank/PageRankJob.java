package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PageRankJob
{
    public static void main(String... args)
    throws IOException
    {
        // handle args

        String inputNodePath;
        String inputEdgePath;
        String outputPath;

        // MAKE THE GRAPH

        {

            JobConf conf = new JobConf(PageRankJob.class);
            conf.setJobName("pagerank-graph-parsing");

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            conf.setMapperClass(PageRankMapper.class);
            conf.setReducerClass(PageRankReducer.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            conf.setNumReduceTasks(8);

        }

        // DO DAT MAPPING AND REDUCING, RANK DEM PAGES

        //conf.set("property", "value");

        /*

            1. parse the edges, nodes from two files
                -> output adjacency lists for each node

            2. do pagerank until satisfied (50 rounds OR convergent)

        */

    }

}


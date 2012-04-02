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

        String inputPath = args[1];
        String outputPath = args[2];


        if (args[0].equals("parse"))
        {
            // MAKE THE GRAPH

            JobConf conf = new JobConf(PageRankJob.class);
            conf.setJobName("pagerank-graph-parsing");

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            conf.setMapperClass(GraphMapper.class);
            conf.setReducerClass(GraphReducer.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            conf.setNumReduceTasks(1);

            FileInputFormat.setInputPaths(conf, new Path(inputPath));
            FileOutputFormat.setOutputPath(conf, new Path(outputPath));
            
            JobClient.runJob(conf);

        }
        else if (args[0].equals("pagerank"))
        {
            for (int count = 0; count < 2; ++count)
            {
                JobConf conf = new JobConf(PageRankJob.class);
                conf.setJobName("pageranking");

                conf.setMapperClass(GraphMapper.class);
                conf.setReducerClass(GraphReducer.class);

                conf.setInputFormat(TextInputFormat.class);
                conf.setOutputFormat(TextOutputFormat.class);

                conf.setNumReduceTasks(8);

                FileInputFormat.setInputPaths(conf, new Path(inputPath));
                FileOutputFormat.setOutputPath(conf, new Path(outputPath + count));
            }
        }

        //conf.set("property", "value");
    }

}


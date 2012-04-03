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

            conf.setMapOutputKeyClass(LongWritable.class);
            conf.setMapOutputValueClass(Text.class);

            conf.setOutputKeyClass(LongWritable.class);
            conf.setOutputValueClass(Node.class);

            conf.setMapperClass(GraphMapper.class);
            conf.setReducerClass(GraphReducer.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(SequenceFileOutputFormat.class);

            conf.setNumReduceTasks(1);

            FileInputFormat.setInputPaths(conf, new Path(inputPath));
            FileOutputFormat.setOutputPath(conf, new Path(outputPath));
            
            JobClient.runJob(conf);

        }
        else if (args[0].equals("pagerank"))
        {
            for (int count = 0; count < 5; ++count)
            {
                JobConf conf = new JobConf(PageRankJob.class);
                conf.setJobName("pageranking");

                conf.setMapOutputKeyClass(LongWritable.class);
                conf.setMapOutputValueClass(Contribution.class);

                conf.setOutputKeyClass(LongWritable.class);
                conf.setOutputValueClass(Node.class);

                conf.setMapperClass(PageRankMapper.class);
                conf.setReducerClass(PageRankReducer.class);

                conf.setInputFormat(SequenceFileInputFormat.class);
                conf.setOutputFormat(SequenceFileOutputFormat.class);

                conf.setNumReduceTasks(1);

                FileInputFormat.setInputPaths(conf, new Path(inputPath));
                FileOutputFormat.setOutputPath(conf, new Path(outputPath + count));

                JobClient.runJob(conf);

                inputPath = outputPath + count;
            }
        }

        //conf.set("property", "value");
    }

}


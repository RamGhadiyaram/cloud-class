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

        Double alpha = Double.valueOf(args[2]);

        String inputPath = args[0];
        String outputGraphPath = args[1] + "/graph";
        String outputTmpPath = args[1] + "/tmp";
        Long numberOfNodes = new Long(1);

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

        //conf.set("mapred.reduce.slowstart.completed.maps", "1.0");

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputGraphPath));
        
        RunningJob job = JobClient.runJob(conf);
        job.waitForCompletion();

        numberOfNodes = job.getCounters().findCounter("NUMBER", "NODES").getCounter();

        inputPath = outputGraphPath;

        for (int count = 0; count < 25; ++count)
        {
            conf = new JobConf(PageRankJob.class);
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

            //conf.set("mapred.reduce.slowstart.completed.maps", "1.0");

            conf.set("numberOfNodes", numberOfNodes.toString());
            conf.set("alpha", alpha.toString());

            FileInputFormat.setInputPaths(conf, new Path(inputPath));
            FileOutputFormat.setOutputPath(conf, new Path(outputTmpPath + count));

            job = JobClient.runJob(conf);
            job.waitForCompletion();

            inputPath = outputTmpPath + count;
        }
    }

}


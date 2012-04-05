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
        Long numberDangling = new Long(1);

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
        numberDangling = job.getCounters().findCounter("NUMBER", "DANGLING").getCounter();

        inputPath = outputGraphPath;

        Double danglingWeight = ((double) numberDangling)/numberOfNodes;
        double amountOfChange = 1.0;

        int round = 0;
        double threshold = 0.01;

        while (amountOfChange >= threshold && round < 3)
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
            conf.set("extraWeight", new Double(danglingWeight/numberOfNodes).toString());

            FileInputFormat.setInputPaths(conf, new Path(inputPath));
            FileOutputFormat.setOutputPath(conf, new Path(outputTmpPath + round));

            job = JobClient.runJob(conf);
            job.waitForCompletion();

            danglingWeight = job.getCounters().findCounter("WEIGHT", "DANGLING").getCounter()
                                / ((double) numberOfNodes * Constants.inflationFactor);
            amountOfChange = job.getCounters().findCounter("WEIGHT", "CONVERGENCE").getCounter()
                                / ((double) numberOfNodes * Constants.inflationFactor);

            String toDelete = new String(inputPath);
            inputPath = outputTmpPath + round;

            System.out.println("Recovering " + danglingWeight + " dangling weight next round.");
            System.out.println("Only changed " + amountOfChange + " this round.");

            FileSystem fs = FileSystem.get(new Configuration());
            //fs.delete(new Path(toDelete), true);

            System.out.println("Deleted old directory (" + toDelete + ")");

            ++round;
        }
    }

}


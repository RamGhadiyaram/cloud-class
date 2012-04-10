package edu.kent.cs.ntietz.kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.*;

public class KMeansJob
{
    public static void main(String... args)
    throws IOException
    {
        int numberOfCenters = Integer.valueOf(args[1]);
        String baseLocation = args[0];

        DataGeneratorJob.main( String.valueOf(numberOfCenters)
                             , String.valueOf(numberOfCenters*1000)
                             , String.valueOf(3)
                             , String.valueOf(0.0)
                             , String.valueOf(100.0)
                             , baseLocation + "/original"
                             );

        int round = 0;

        String currentLocation = baseLocation + "/original";
        String nextLocation = baseLocation + "/round" + round;

        while (round < 5)
        {
            JobConf conf = new JobConf(KMeansJob.class);
            conf.setJobName("kmeans");

            conf.setMapOutputKeyClass(LongWritable.class);
            conf.setMapOutputValueClass(Point.class);
            conf.setOutputKeyClass(LongWritable.class);
            conf.setOutputValueClass(Point.class);

            conf.setMapperClass(KMeansMapper.class);
            conf.setReducerClass(KMeansReducer.class);

            conf.setInputFormat(SequenceFileInputFormat.class);
            conf.setOutputFormat(SequenceFileOutputFormat.class);

            conf.setNumReduceTasks(1);

            conf.set("mapred.reduce.slowstart.completed.maps", "1.0");
            conf.set("numberOfCenters", String.valueOf(numberOfCenters));
            conf.set("centersReadDirectory", currentLocation);
            conf.set("centersWriteDirectory", nextLocation);

            FileInputFormat.setInputPaths(conf, new Path(currentLocation+"/points"));
            FileOutputFormat.setOutputPath(conf, new Path(nextLocation+"/points"));

            RunningJob job = JobClient.runJob(conf);
            job.waitForCompletion();

            ++round;

            currentLocation = nextLocation;
            nextLocation = baseLocation + "/round" + round;
        }
    }
}


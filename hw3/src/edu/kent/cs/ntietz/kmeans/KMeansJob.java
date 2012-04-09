package edu.kent.cs.ntietz.kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.*;

public class KMeansJob
{
    public static void main(String... args)
    {
        // TODO handle args here
        String inputDirectory = args[0];
        String outputDirectory = args[0];
        int numberOfCenters = 0; // TODO

        int round = 0;

        while (round < 1)
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
            conf.set("outputDirectory", outputDirectory);

            FileInputFormat.setInputPaths(conf, new Path(inputDirectory));
            FileOutputFormat.setOutputPath(conf, new Path(outputDirectory+"/points/"+round));

            ++round;
        }
    }
}


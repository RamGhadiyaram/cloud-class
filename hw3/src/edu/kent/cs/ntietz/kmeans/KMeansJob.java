package edu.kent.cs.ntietz.kmeans;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.*;

public class KMeansJob
{
    public static void main(String... args)
    {
        // TODO handle args here
        String inputPath = args[0];


        JobConf conf = new JobConf(KMeansJob.class);
        conf.setJobName("kmeans");

        /*
        conf.setMapOutputKeyClass(...);
        conf.setMapOutputValueClass(...);
        conf.setOutputKeyClass(...);
        conf.setOutputValueClass(...);

        conf.setMapperClass(...);
        conf.setReducerClass(...);
        */

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        //FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    }
}


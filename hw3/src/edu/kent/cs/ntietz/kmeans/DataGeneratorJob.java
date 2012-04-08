package edu.kent.cs.ntietz.kmeans;

import java.io.*;
import java.util.*;

public class DataGeneratorJob
{
    public static void main(String... args)
    throws IOException
    {
        // TODO handle args nicely here. perhaps import apache commons for arg handling.

        int numberOfCenters = Integer.valueOf(args[0]);
        int numberOfPoints = Integer.valueOf(args[1]);
        int numberOfComponents = Integer.valueOf(args[2]);
        double lowerBound = Double.valueOf(args[3]);
        double upperBound = Double.valueOf(args[4]);
        long seed = 2012; // TODO change this, keep it fixed for testing

        // TODO replace the rudimentary arg handling above

        DataGenerator gen = new DataGenerator();

        List<Point> dataset = gen.generate( numberOfCenters
                                          , numberOfPoints
                                          , numberOfComponents
                                          , lowerBound
                                          , upperBound
                                          , seed
                                          );

        //JobConf conf = new JobConf(DataGeneratorJob.class);
        //conf.setJobName("kmeans-data-generation");

        /*
        conf.setMapOutputKeyClass(...);
        conf.setMapOutputValueClass(...);
        conf.setOutputKeyClass(...);
        conf.setOutputValueClass(...);

        conf.setMapperClass(...);
        conf.setReducerClass(...);

        conf.setInputFormat(...);
        conf.setOutputFormat(...);
        */
    }
}


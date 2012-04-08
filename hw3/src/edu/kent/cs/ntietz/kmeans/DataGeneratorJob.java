package edu.kent.cs.ntietz.kmeans;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

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

        String outputPath = args[5];

        // TODO replace the rudimentary arg handling above

        DataGenerator gen = new DataGenerator();

        System.out.println("Generating data...");

        List<Point> dataset = gen.generate( numberOfCenters
                                          , numberOfPoints
                                          , numberOfComponents
                                          , lowerBound
                                          , upperBound
                                          , seed
                                          );

        System.out.println("Done generating data.");

        //for (Point each : dataset)
        //{
        //    System.out.println(each);
        //}

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Writer writer =
            new SequenceFile.Writer( fs
                                   , conf
                                   , new Path(outputPath)
                                   , LongWritable.class
                                   , Point.class
                                   );

        System.out.println("Writing to " + outputPath + ".");

        for (int index = 0; index < numberOfPoints; ++index)
        {
            writer.append(new LongWritable(index), dataset.get(index));
        }

        writer.close();

        System.out.println("Finished writing.");
        
    }
}


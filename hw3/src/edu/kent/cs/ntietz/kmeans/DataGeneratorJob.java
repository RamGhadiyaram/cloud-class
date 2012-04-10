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
        int numberOfCenters = Integer.valueOf(args[0]);
        int numberOfPoints = Integer.valueOf(args[1]);
        int numberOfComponents = Integer.valueOf(args[2]);
        double lowerBound = Double.valueOf(args[3]);
        double upperBound = Double.valueOf(args[4]);
        long seed = 2012;
        String outputPath = args[5];

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

        fs.delete(new Path(outputPath), true);

        SequenceFile.Writer writer =
            new SequenceFile.Writer( fs
                                   , conf
                                   , new Path(outputPath + "/points")
                                   , LongWritable.class
                                   , Point.class
                                   );

        System.out.println("Writing dataset to " + outputPath + "/points.");

        for (int index = 0; index < numberOfPoints; ++index)
        {
            writer.append(new LongWritable(index), dataset.get(index));
        }

        writer.close();

        System.out.println("Finished writing.");
        
        System.out.println("Writing centers to " + outputPath + "/centers.");

        
        for (int index = 0; index < numberOfCenters; ++index)
        {
            writer =
                new SequenceFile.Writer( fs
                                       , conf
                                       , new Path(outputPath + "/centers/" + index)
                                       , LongWritable.class
                                       , Point.class
                                       );

            writer.append(new LongWritable(index), dataset.get(index));
            writer.close();
        }
    }
}


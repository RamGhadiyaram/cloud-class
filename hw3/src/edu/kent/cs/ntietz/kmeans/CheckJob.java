package edu.kent.cs.ntietz.kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

import java.io.*;
import java.util.*;

public class CheckJob
{
    public static void main(String... args)
    throws IOException
    {
        String inputPath = args[0];
        int numberOfCenters = Integer.valueOf(args[1]);

        for (Point each : getCenters(inputPath, numberOfCenters))
        {
            System.out.println(each.toString());
        }
    }

    public static List<Point> getCenters(String basePath, int numberOfCenters)
    throws IOException
    {
        List<Point> centers = new ArrayList<Point>(numberOfCenters);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        for (int index = 0; index < numberOfCenters; ++index)
        {
            SequenceFile.Reader reader =
                new SequenceFile.Reader( fs
                                       , new Path(basePath + "/centers/" + index)
                                       , conf
                                       );

            LongWritable key = new LongWritable();
            Point value = new Point();

            reader.next(key, value);

            centers.add(value);
        }

        return centers;
    }
}

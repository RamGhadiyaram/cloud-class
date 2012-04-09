package edu.kent.cs.ntietz.kmeans;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.*;

public class KMeansReducer
extends MapReduceBase
implements Reducer<LongWritable, Point, LongWritable, Point>
{

    private int numberOfCenters = 0;
    private List<Point> centers = new ArrayList<Point>();
    private String outputDirectory;

    // reduce phase

    // input: a center and all its points
    // output: all the points with their center
    // side-effect: update the centers

    // centers are stored in : outputdir + "/centers/" + centerNumber

    public void configure(JobConf conf)
    {
        outputDirectory = conf.get("outputDirectory");
    }

    public void reduce( LongWritable key
                      , Iterator<Point> values
                      , OutputCollector<LongWritable, Point> output
                      , Reporter reporter
                      )
    throws IOException
    {
        Point first = values.next();
        Point sums = new Point(first.cardinality());
        long count = 0;

        sums = Point.add(sums, first);

        while (values.hasNext())
        {
            Point each = values.next();

            sums = Point.add(sums, each);
            ++count;

            output.collect(key, each);
        }

        // create a new center
        Point center = Point.divide(sums, count);

        // write out the new center
        Configuration c = new Configuration();
        FileSystem fs = FileSystem.get(c);

        for (int index = 0; index < numberOfCenters; ++index)
        {
            SequenceFile.Writer writer =
                new SequenceFile.Writer( fs
                                       , c
                                       , new Path(outputDirectory + "/centers/" + index)
                                       , LongWritable.class
                                       , Point.class
                                       );


        }
    }

}


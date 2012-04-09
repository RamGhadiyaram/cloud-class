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
    private String centersDirectory;

    // reduce phase

    // input: a center and all its points
    // output: all the points with their center
    // side-effect: update the centers

    // centers are stored in : outputdir + "/centers/" + centerNumber

    public void configure(JobConf conf)
    {
        centersDirectory = conf.get("centersDirectory");
    }

    public void reduce( LongWritable key
                      , Iterator<Point> values
                      , OutputCollector<LongWritable, Point> output
                      , Reporter reporter
                      )
    throws IOException
    {
        double average = 0.0;
        long count = 0;

        // create a list of values
        // create a running-average of the values in the iterator
        /*for (Point each : values)
        {
            ++count;

            double sumOfSquares = each.sumOfSquares(center);
            average = ((double)count-1)/count * average + 0.0; //TODO finish
        }*/
        // while doing this, output the unaltered values

        // create a new center

        // write out the new center
        /*
        Configuration c = new Configuration();
        FileSystem fs = FileSystem.get(c);

        for (int index = 0; index < numberOfCenters; ++index)
        {
            SequenceFile.Writer writer =
                new SequenceFile.Writer( fs
                                       , c
                                       , new Path(outputDir + "/centers/" + index)
                                       , LongWritable.class
                                       , Point.class
                                       );
        }
        */
    }

}


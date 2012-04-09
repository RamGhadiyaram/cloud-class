package edu.kent.cs.ntietz.kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.*;

public class KMeansMapper
extends MapReduceBase
implements Mapper<LongWritable, Point, LongWritable, Point>
{
    private int numberOfCenters = 0;
    private List<Point> centers = new ArrayList<Point>();
    private String centersDirectory;

    // map :: [(long, point)] -> [(long, point)]
    // each long corresponds to which center it is closest to

    // in the map phase, we find which center each point is closest to.
    // they default to center "0"

    // input: a point and its current center
    // output: that point and its closest center

    public void configure(JobConf conf)
    {
        numberOfCenters = Integer.valueOf(conf.get("numberOfCenters"));
        centersDirectory = conf.get("centersDirectory");
        
        try
        {
            Configuration c = new Configuration();
            FileSystem fs = FileSystem.get(c);

            for (int index = 0; index < numberOfCenters; ++index)
            {
                SequenceFile.Reader reader =
                    new SequenceFile.Reader( fs
                                           , new Path(centersDirectory)
                                           , c
                                           );
            }
        }
        catch (IOException e)
        {
            // do nothing
            // I hope this doesn't happen
        }
    }

    public void map( LongWritable key
                   , Point value
                   , OutputCollector<LongWritable, Point> output
                   , Reporter reporter
                   )
    throws IOException
    {
        double min = value.sumOfSquares(centers.get(0));
        int best = 0;

        for (int index = 1; index < numberOfCenters; ++index)
        {
            double current = value.sumOfSquares(centers.get(index));

            if (current < min)
            {
                min = current;
                best = index;
            }
        }

        output.collect(new LongWritable(best), value);
    }

}


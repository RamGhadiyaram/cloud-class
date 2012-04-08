package edu.kent.cs.ntietz.kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class KMeansMapper
extends MapReduceBase
implements Mapper<LongWritable, Point, LongWritable, Point>
{

}


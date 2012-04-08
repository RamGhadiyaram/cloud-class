package edu.kent.cs.ntietz.kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class KMeansReducer
extends MapReduceBase
implements Reducer<LongWritable, Point, LongWritable, Point>
{

}


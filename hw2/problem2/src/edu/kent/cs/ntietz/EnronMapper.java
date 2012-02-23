package edu.kent.cs.ntietz;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class EnronMapper
extends MapReduceBase
implements Mapper<LongWritable, Text, KeyPair, IntWritable>
{
    public void map(LongWritable key, Text value, OutputCollector<KeyPair, IntWritable> output, Reporter reporter)
    {

    }
}


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

public class EnronReducer
extends MapReduceBase
implements Reducer<KeyPair, IntWritable, Text, Text>
{
    public void reduce(KeyPair key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
    {

    }
}


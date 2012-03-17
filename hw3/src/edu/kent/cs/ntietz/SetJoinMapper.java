package edu.kent.cs.ntietz;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SetJoinMapper
extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>
{
    public void map( LongWritable key
                   , Text value
                   , OutputCollector<Text, Text> output
                   , Reporter reporter
                   )
    throws IOException
    {
        output.collect(new Text(String.valueOf(key)), value);
    }
}


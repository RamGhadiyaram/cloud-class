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

public class LinePairs
{
    public static void main(String... args)
    throws IOException
    {
        JobConf conf = new JobConf(LinePairs.class);
        conf.setJobName("parta-pairs");

        conf.setOutputKeyClass(KeyPair.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(PairMapper.class);
        conf.setReducerClass(PairReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumReduceTasks(8);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        FileOutputFormat.setCompressOutput(conf, false); 

        JobClient.runJob(conf);
    }
}


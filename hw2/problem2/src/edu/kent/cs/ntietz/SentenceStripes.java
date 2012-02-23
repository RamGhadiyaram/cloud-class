package edu.kent.cs.ntietz;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SentenceStripes
{
    public static void main(String... args)
    throws IOException
    {
        JobConf conf = new JobConf(SentenceStripes.class);
        conf.setJobName("partb-stripes");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(MapWritable.class);

        conf.setMapperClass(StripeMapper.class);
        conf.setReducerClass(StripeReducer.class);

        conf.setInputFormat(SentenceInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumReduceTasks(8);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        FileOutputFormat.setCompressOutput(conf, false);

        JobClient.runJob(conf);
    }
}


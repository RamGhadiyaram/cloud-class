package edu.kent.cs.ntietz;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SetJoinJob
{
    public static void main(String... args)
    throws IOException
    {
        JobConf conf = new JobConf(SetJoinJob.class);
        conf.setJobName("union-job");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputKeyClass(Text.class);

        conf.setMapperClass(Mapper.class);
        conf.setReducerClass(Reducer.class);

        conf.setInputFormat(TextInputFormat.class);

        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumReduceTasks(8);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[0]));
        FileOutputFormat.setCompressOutput(conf, false);

        JobClient.runJob(conf);
    }
}


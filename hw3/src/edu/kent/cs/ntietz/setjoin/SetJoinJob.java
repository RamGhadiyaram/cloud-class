package edu.kent.cs.ntietz.setjoin;

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
        if (args.length < 4)
        {
            // output usage
            // quit
        }

        String minSimilarity = args[0];
        String smallFile = args[1];
        String largeFile = args[2];
        String outputLoc = args[3];

        JobConf conf = new JobConf(SetJoinJob.class);
        conf.setJobName("union-job");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputKeyClass(Text.class);

        conf.setMapperClass(SetJoinMapper.class);
        conf.setReducerClass(SetJoinReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumReduceTasks(8);

        conf.set("smallFilePath", smallFile);
        conf.set("minisim", minSimilarity);

        FileInputFormat.setInputPaths(conf, new Path(largeFile));
        FileOutputFormat.setOutputPath(conf, new Path(outputLoc));
        FileOutputFormat.setCompressOutput(conf, false);

        JobClient.runJob(conf);
    }
}


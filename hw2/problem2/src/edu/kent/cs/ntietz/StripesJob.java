package edu.kent.cs.ntietz;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class StripesJob
{
    public static void main(String... args)
    throws IOException
    {
        JobConf conf = new JobConf(LineStripes.class);
        conf.setJobName("parta-stripes");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(MapWritable.class);

        conf.setMapperClass(StripeMapper.class);
        conf.setReducerClass(StripeReducer.class);

        if (args[0].equals("line")) {
            conf.setInputFormat(TextInputFormat.class);
        } else if (args[0].equals("sentence")) {
            conf.setInputFormat(SentenceInputFormat.class);
        } else if (args[0].equals("paragraph")) {
            conf.setInputFormat(ParagraphInputFormat.class);
        }

        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumReduceTasks(8);

        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
        FileOutputFormat.setCompressOutput(conf, false);

        JobClient.runJob(conf);
    }
}


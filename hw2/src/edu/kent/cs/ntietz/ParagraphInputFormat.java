package edu.kent.cs.ntietz;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/* This input format supports reading one sentence at a time.
 */
public class ParagraphInputFormat
    extends FileInputFormat<LongWritable, Text>
{
    public RecordReader<LongWritable, Text> getRecordReader( InputSplit split
                                                           , JobConf conf
                                                           , Reporter reporter
                                                           )
    throws IOException
    {
        return new ParagraphRecordReader(conf, (FileSplit)split);
    }

}


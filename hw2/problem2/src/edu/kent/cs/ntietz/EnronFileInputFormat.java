package edu.kent.cs.ntietz;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;

/*
 * Like the normal file input format, but it doesn't allow files to be split.
 * Each mapper will handle a whole file.
 */
public class EnronFileInputFormat extends FileInputFormat
{
    protected boolean isSplitable(FileSystem fs, Path filename)
    {
        return false;
    }
    
    public RecordReader<LongWritable, Text> getRecordReader( InputSplit split
                                                           , JobConf conf
                                                           , Reporter reporter
                                                           )
    throws IOException
    {
        return new EnronRecordReader(conf, (FileSplit)split);
    }


}


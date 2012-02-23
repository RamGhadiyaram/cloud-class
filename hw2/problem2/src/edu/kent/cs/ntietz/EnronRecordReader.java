package edu.kent.cs.ntietz;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class EnronRecordReader 
    implements RecordReader<LongWritable, Text>
{
    private LineRecordReader recordReader;

    public EnronRecordReader( Configuration conf
                            , FileSplit split
                            )
    throws IOException
    {
        recordReader = new LineRecordReader(conf, split);
    }

    public EnronRecordReader( InputStream in
                            , long offset
                            , long endOffset
                            , int maxLineLength
                            )
    {
        recordReader = new LineRecordReader(in, offset, endOffset, maxLineLength);
    }

    public EnronRecordReader( InputStream in
                            , long offset
                            , long endOffset
                            , Configuration conf
                            )
    throws IOException
    {
        recordReader = new LineRecordReader(in, offset, endOffset, conf);
    }

    public void close()
    throws IOException
    {
        recordReader.close();
    }

    public LongWritable createKey()
    {
        return recordReader.createKey();
    }

    public Text createValue()
    {
        return recordReader.createValue();
    }

    public long getPos()
    throws IOException
    {
        return recordReader.getPos();
    }

    public float getProgress()
    {
        return recordReader.getProgress();
    }

    /* 
     * Finds a full file and sets it as the value.
     */
    public synchronized boolean next(LongWritable key, Text value)
    throws IOException
    {
        Text line = new Text();
        boolean retrieved = true;

        String result = "";

        value.clear();

        while ( retrieved )
        {
            retrieved = recordReader.next(key, line);

            if (line.toString().length() > 0)
            {
                String lineValue = line.toString();
                result += " " + lineValue;
            }
        }

        value.set(result);
        return true;
    }
}


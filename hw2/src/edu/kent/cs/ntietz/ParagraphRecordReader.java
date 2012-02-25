package edu.kent.cs.ntietz;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;




public class ParagraphRecordReader 
    implements RecordReader<LongWritable, Text>
{
    private LineRecordReader recordReader;

    public ParagraphRecordReader( Configuration conf
                               , FileSplit split
                               )
    throws IOException
    {
        recordReader = new LineRecordReader(conf, split);
    }

    public ParagraphRecordReader( InputStream in
                               , long offset
                               , long endOffset
                               , int maxLineLength
                               )
    {
        recordReader = new LineRecordReader(in, offset, endOffset, maxLineLength);
    }

    public ParagraphRecordReader( InputStream in
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

    /* Finds a full paragraph and sets it as the value.
     */
    public synchronized boolean next(LongWritable key, Text value)
    throws IOException
    {
        Text line = new Text();
        boolean getMore = true;
        boolean retrieved = false;
        boolean gotSomething = false;

        String result = "";

        value.clear();

        while ( getMore )
        {
            line.clear();
            retrieved = recordReader.next(key, line);

            if (line.toString().length() > 0)
            {
                String lineValue = line.toString();
                result += " " + lineValue;
                gotSomething = true;
            }
            else
            {
                getMore = false;
            }
        }

        value.set(result);
        return gotSomething;
    }
}


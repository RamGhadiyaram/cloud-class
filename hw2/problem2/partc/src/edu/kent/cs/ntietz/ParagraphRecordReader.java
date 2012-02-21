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

    /* Finds a full sentence and sets it as the value.
     * If the sentence is shorter than the full line, the rest is stored to use later.
     */
    public synchronized boolean next(LongWritable key, Text value)
    throws IOException
    {
        Text line = new Text();
        boolean getMore = true;
        boolean retrieved = false;

        String result = "";

        value.clear();

        while ( getMore )
        {
            retrieved = recordReader.next(key, line);

            if (!(line.toString().trim().isEmpty()))
            {
                String lineValue = line.toString();

                result += " " + lineValue;
            }
            else
            {
                getMore = false;
                value.set(result);
                return false;
            }
        }

        value.set(result);
        return true;
    }
}


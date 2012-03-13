package edu.kent.cs.ntietz;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SetJoinReducer
extends MapReduceBase
implements Reducer<Text, Text, Text, Text>
{
    public void reduce( Text key
                      , Iterator<Text> values
                      , OutputCollector<Text, Text> output
                      , Reporter reporter
                      )
    throws IOException
    {

    }
}


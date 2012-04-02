package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;

public class AdjacencyList
implements Writable
{
    public List<String> neighbors;

    public void write(DataOutput out)
    throws IOException
    {
        out.writeInt(neighbors.size());
        for (String each : neighbors)
        {
            out.writeUTF(each);
        }
    }

    public void readFields(DataInput in)
    throws IOException
    {
        int listSize = in.readInt();
        neighbors = new ArrayList<String>(listSize);
        for (int index = 0; index < listSize; ++index)
        {
            neighbors.add(in.readUTF());
        }
    }
}


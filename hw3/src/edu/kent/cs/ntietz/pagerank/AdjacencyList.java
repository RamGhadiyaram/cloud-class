package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;

public class AdjacencyList
implements Writable
{
    public List<String> members = new ArrayList<String>();

    public void write(DataOutput out)
    throws IOException
    {
        out.writeInt(members.size());
        for (String each : members)
        {
            out.writeUTF(each);
        }
    }

    public void readFields(DataInput in)
    throws IOException
    {
        int listSize = in.readInt();
        members = new ArrayList<String>(listSize);
        for (int index = 0; index < listSize; ++index)
        {
            members.add(in.readUTF());
        }
    }
}


package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;

public class AdjacencyList
implements WritableComparable
{
    public String node;
    public List<String> neighbors;

    public void write(DataOutput out)
    throws IOException
    {
        out.writeUTF(node);
        out.writeInt(neighbors.size());
        for (String each : neighbors)
        {
            out.writeUTF(each);
        }
    }

    public void readFields(DataInput in)
    throws IOException
    {
        node = in.readUTF();

        int listSize = in.readInt();
        neighbors = new ArrayList<String>(listSize);
        for (int index = 0; index < listSize; ++index)
        {
            neighbors.add(in.readUTF());
        }
    }

    public int compareTo(Object obj)
    {
        if (obj instanceof AdjacencyList)
        {
            AdjacencyList other = (AdjacencyList) obj;

            return node.compareTo(other.node);
        }
        else
        {
            return -1;
        }
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof AdjacencyList)
        {
            AdjacencyList other = (AdjacencyList) obj;

            return node.equals(other.node)
                && neighbors.containsAll(other.neighbors)
                && other.neighbors.containsAll(neighbors);
        }
        else
        {
            return false;
        }
    }

    public int hashCode()
    {
        return node.hashCode();
    }
}


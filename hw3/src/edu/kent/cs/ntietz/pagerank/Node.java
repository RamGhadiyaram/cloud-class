package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;

public class Node
implements Writable
{
    public static final int numberOfNodes = 1131681;
    public static final double defaultWeight = 1.0 / numberOfNodes;

    public String name;
    public double score;
    public AdjacencyList neighbors = new AdjacencyList();

    public void write(DataOutput out)
    throws IOException
    {
        out.writeUTF(name);
        out.writeDouble(score);
        neighbors.write(out);
    }

    public void readFields(DataInput in)
    throws IOException
    {
        name = in.readUTF();
        score = in.readDouble();
        neighbors.readFields(in);
    }
/*
    public int compareTo(Object obj)
    {
        if (obj instanceof Node)
        {
            Node other = (Node) obj;

            return name.compareTo(other.name);
        }
        else
        {
            return -1;
        }
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof Node)
        {
            Node other = (Node) obj;

            return name.equals(other.name);
        }
        else
        {
            return false;
        }
    }

    public int hashCode()
    {
        return name.hashCode();
    }
*/
}


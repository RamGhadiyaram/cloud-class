package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;

public class Node
implements WritableComparable
{
    public String name;

    public void write(DataOutput out)
    throws IOException
    {
        out.writeUTF(name);
    }

    public void readFields(DataInput in)
    throws IOException
    {
        name = in.readUTF();
    }

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
}


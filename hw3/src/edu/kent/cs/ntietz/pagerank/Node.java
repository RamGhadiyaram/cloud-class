package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;

public class Node
implements Writable
{
    public static final double DEFAULT = -1.0;

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

    public String toString()
    {
        String result = "";

        result += "name: " + name;
        result += ";\t";

        result += "score: " + score;
        result += ";\t";

        result += "neighbors: ";
        for (String each : neighbors.members)
        {
            result += each + " ";
        }
        result += "\t";

        return result;
    }

    public boolean isDangling()
    {
        return neighbors.members.size() == 0;
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


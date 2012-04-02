package edu.kent.cs.ntietz.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;

public class Contribution
implements Writable
{
    public boolean isScore = true;
    public String name;
    public double score = 0.0;
    public Node node = null;

    public void write(DataOutput out)
    throws IOException
    {
        out.writeBoolean(isScore);
        out.writeUTF(name);
        if (isScore)
        {
            out.writeDouble(score);
        }
        else
        {
            node.write(out);
        }
    }

    public void readFields(DataInput in)
    throws IOException
    {
        isScore = in.readBoolean();
        name = in.readUTF();
        if (isScore)
        {
            score = in.readDouble();
        }
        else
        {
            node = new Node();
            node.readFields(in);
        }
    }
/*
    public int compareTo(Object obj)
    {
        if (obj instanceof Contribution)
        {
            Contribution other = (Contribution) obj;

            if (name.equals(other.name) && !isScore)
            {
                return -1;
            }
            else
            {
                return name.compareTo(other.name);
            }
        }
        else
        {
            return -1;
        }
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof Contribution)
        {
            Contribution other = (Contribution) obj;

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


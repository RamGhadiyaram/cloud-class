package edu.kent.cs.ntietz;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class KeyPair implements WritableComparable
{
    public String left;
    public String right;
    public boolean marginal = false;

    public void write(DataOutput out)
    throws IOException
    {
        out.writeBoolean(marginal);
        out.writeUTF(left);
        out.writeUTF(right);
    }

    public void readFields(DataInput in)
    throws IOException
    {
        marginal = in.readBoolean();
        left = in.readUTF();
        right = in.readUTF();
    }

    public int compareTo(Object oOther)
    {
        if (oOther instanceof KeyPair)
        {
            KeyPair other = (KeyPair)oOther;

            if (! left.equals(other.left))
                return left.compareTo(other.left);

            if (marginal)
            {
                if (other.marginal)
                    return 0;
                else
                    return -1;
            }
            
            if (other.marginal)
                return 1;

            else
                return right.compareTo(other.right);
        }
        else
        {
            return -1;
        }
    }

    public boolean equals(Object oOther)
    {
        if (oOther instanceof KeyPair)
        {
            KeyPair other = (KeyPair)oOther;
            return left.equals(other.left) && right.equals(other.right);
        }
        else
        {
            return false;
        }
    }

    public int hashCode()
    {
        return left.hashCode();
    }
}

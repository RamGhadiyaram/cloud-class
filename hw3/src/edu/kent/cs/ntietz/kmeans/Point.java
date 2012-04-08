package edu.kent.cs.ntietz.kmeans;

import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;

public class Point
implements Writable
{
    private List<Double> components;
    int numberOfComponents;

    public Point(int n)
    {
        numberOfComponents = n;
        components = new ArrayList<Double>(numberOfComponents);
    }

    public Point(List<Double> c)
    {
        components = c;
        numberOfComponents = components.size();
    }
    
    Double getComponent(int i)
    {
        return components.get(i);
    }

    void setComponent(int i, Double v)
    {
        components.set(i, v);
    }

    public int cardinality()
    {
        return components.size();
    }

    public void write(DataOutput out)
    throws IOException
    {
        out.writeInt(numberOfComponents);
        for (int index = 0; index < numberOfComponents; ++index)
        {
            out.writeDouble(components.get(index));
        }
    }

    public void readFields(DataInput in)
    throws IOException
    {
        numberOfComponents = in.readInt();
        components = new ArrayList<Double>(numberOfComponents);
        for (int index = 0; index < numberOfComponents; ++index)
        {
            double value = in.readDouble();
            components.add(value);
        }
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof Point)
        {
            Point other = (Point) obj;

            if (cardinality() != other.cardinality())
            {
                return false;
            }
            else
            {
                for (int index = 0; index < cardinality(); ++index)
                {
                    if (!getComponent(index).equals(other.getComponent(index)))
                    {
                        return false;
                    }
                }

                return true;
            }
        }
        else
        {
            return false;
        }
    }
}


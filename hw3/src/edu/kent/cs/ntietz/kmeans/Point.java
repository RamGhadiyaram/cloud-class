package edu.kent.cs.ntietz.kmeans;

import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;

public class Point
implements WritableComparable
{
    private List<Double> components;
    int numberOfComponents;

    public Point(int n)
    {
        numberOfComponents = n;
        components = new ArrayList<Double>(numberOfComponents);
        for (int index = 0; index < numberOfComponents; ++index)
        {
            components.add(0.0);
        }
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
        return numberOfComponents;
    }

    public double distance(Point other)
    {
        return Math.sqrt(sumOfSquares(other));
    }

    public double sumOfSquares(Point other)
    {
        double sum = 0.0;

        for (int index = 0; index < numberOfComponents; ++index)
        {
            double difference = getComponent(index) - other.getComponent(index);
            sum += difference * difference;
        }

        return sum;
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

    public String toString()
    {
        String result = "";
        
        result += "size: " + numberOfComponents + "; ";

        result += "components: ";
        for (double each : components)
        {
            result += each + "\t";
        }

        return result;
    }

    public int compareTo(Object obj)
    {
        if (obj instanceof Point)
        {
            Point other = (Point) obj;

            if (cardinality() != other.cardinality())
            {
                return -1;
            }
            else
            {
                for (int index = 0; index < numberOfComponents; ++index)
                {
                    if (getComponent(index) < other.getComponent(index))
                    {
                        return -1;
                    }
                    else if (getComponent(index) > other.getComponent(index))
                    {
                        return 1;
                    }
                }
                return 0;
            }
        }
        else
        {
            return -1;
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


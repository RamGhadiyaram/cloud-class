package edu.kent.cs.ntietz.kmeans;

import java.util.*;

public class Point<T>
{
    private List<T> components;
    int numberOfComponents;

    public Point(int n)
    {
        numberOfComponents = n;
        components = new ArrayList<T>(numberOfComponents);
    }

    public Point(List<T> c)
    {
        components = c;
        numberOfComponents = components.size();
    }
    
    T getComponent(int i)
    {
        return components.get(i);
    }

    void setComponent(int i, T v)
    {
        components.set(i, v);
    }

}


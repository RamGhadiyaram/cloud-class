package edu.kent.cs.ntietz.kmeans;

import java.util.*;

public class DataGenerator
{

    public List<Point> generate( int numberOfCenters
                               , int numberOfPoints
                               , int numberOfComponents
                               , double lowerBound
                               , double upperBound
                               , long seed
                               )
    {
        Random random = new Random(seed);

        List<Point> centers = new ArrayList<Point>(numberOfCenters);
        List<Point> points = new ArrayList<Point>(numberOfPoints);

        for (int count = 0; count < numberOfCenters; ++count)
        {
            // TODO: generate each center better.

            Point point = new Point(numberOfComponents);

            for (int index = 0; index < numberOfComponents; ++index)
            {
                double value = lowerBound + (random.nextDouble() * (upperBound - lowerBound));
                point.setComponent(index, value);
            }

            centers.add(point);
        }

        for (int count = 0; count < numberOfPoints; ++count)
        {
            // TODO: generate each point better.

            Point point = new Point(numberOfComponents);

            for (int index = 0; index < numberOfComponents; ++index)
            {
                double value = lowerBound + (random.nextDouble() * (upperBound - lowerBound));
                point.setComponent(index, value);
            }

            points.add(point);
        }

        return points;
    }

}


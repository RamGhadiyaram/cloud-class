package edu.kent.cs.ntietz.kmeans;

import java.util.*;

public class DataGenerator
{

    public List<Point<Double>> generate( int numberOfCenters
                                       , int numberOfPoints
                                       , int numberOfComponents
                                       , double lowerBound
                                       , double upperBound
                                       , long seed
                                       )
    {
        Random random = new Random(seed);

        List<Point<Double>> centers = new ArrayList<Point<Double>>(numberOfCenters);
        List<Point<Double>> points = new ArrayList<Point<Double>>(numberOfPoints);

        for (int count = 0; count < numberOfCenters; ++count)
        {
            // TODO: generate each center better.

            Point<Double> point = new Point<Double>(numberOfComponents);

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

            Point<Double> point = new Point<Double>(numberOfComponents);

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


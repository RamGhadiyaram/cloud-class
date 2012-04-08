package edu.kent.cs.ntietz.kmeans;

import java.util.*;

public class DataGenerator
{
    private static final double clusterThreshold = 0.90; // 70% of points will be around centers

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
            int centerIndex = random.nextInt(numberOfCenters);
            Point center = centers.get(centerIndex);

            Point point = new Point(numberOfComponents);

            double score = random.nextDouble();

            if (score < clusterThreshold)
            {
                for (int index = 0; index < numberOfComponents; ++index)
                {
                    double value = lowerBound + (center.getComponent(index) + random.nextGaussian()*(upperBound-lowerBound)/12);
                    while (value < lowerBound) value += (upperBound - lowerBound);
                    while (value > upperBound) value -= (upperBound - lowerBound);
                    point.setComponent(index, value);
                }
            }
            else
            {
                for (int index = 0; index < numberOfComponents; ++index)
                {
                    double value = lowerBound + (random.nextDouble() * (upperBound - lowerBound));
                    point.setComponent(index, value);
                }
            }

            points.add(point);
        }

        return points;
    }

}


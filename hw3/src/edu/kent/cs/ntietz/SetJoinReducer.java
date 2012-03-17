package edu.kent.cs.ntietz;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SetJoinReducer
extends MapReduceBase
implements Reducer<Text, Text, Text, Text>
{
    private float minSimilarity;

    private String smallFile;
    private List<Set<String>> leftSets = new ArrayList<Set<String>>();

    private boolean first = true;

    public void configure(JobConf conf)
    {
        smallFile = conf.get("smallFilePath");
        minSimilarity = Float.valueOf(conf.get("minisim"));
    }


    public void reduce( Text key
                      , Iterator<Text> values
                      , OutputCollector<Text, Text> output
                      , Reporter reporter
                      )
    throws IOException
    {
        if (first)
        {
            first = false;

            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(smallFile))));

            String line = in.readLine();

            while (line != null)
            {
                Set<String> current = new HashSet<String>();

                StringTokenizer tokens = new StringTokenizer(line);
                while (tokens.hasMoreTokens())
                {
                    current.add(tokens.nextToken());
                }

                leftSets.add(current);

                line = in.readLine();
            }
        }

        while (values.hasNext())
        {
            Text value = values.next();
            Set<String> rightSet = new HashSet<String>();
            StringTokenizer tokens = new StringTokenizer(value.toString());
            while (tokens.hasMoreTokens())
            {
                rightSet.add(tokens.nextToken());
            }

            for (Set<String> leftSet : leftSets)
            {
                Set<String> intersection = new HashSet<String>(leftSet);
                intersection.retainAll(rightSet);

                int intersectionSize = intersection.size();
                int unionSize = leftSet.size() + rightSet.size() - intersectionSize;

                float similarity = ((float)intersectionSize)/unionSize;

                if (similarity >= minSimilarity)
                {
                    Set<String> joined = new HashSet<String>(leftSet);
                    leftSet.addAll(rightSet);

                    String outText = "sim: " + similarity + ", minsim: " + minSimilarity + ". ";

                    for (String each : joined)
                    {
                        outText += each + " ";
                    }

                    output.collect(new Text(String.valueOf(key)), new Text(outText));
                }
            }
        }

    }
}


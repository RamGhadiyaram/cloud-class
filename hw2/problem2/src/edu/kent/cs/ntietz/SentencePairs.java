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

public class SentencePairs
{
    public static class KeyPair implements WritableComparable
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

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, KeyPair, IntWritable>
    {
        public void map(LongWritable key, Text value, OutputCollector<KeyPair, IntWritable> output, Reporter reporter)
        throws IOException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            List<String> words = new ArrayList<String>();

            while (tokenizer.hasMoreTokens())
            {
                words.add(tokenizer.nextToken());
            }

            for (int i = 0; i < words.size(); ++i)
            {
                int count = 0;
                for (int j = 0; j < words.size(); ++j)
                {
                    if (i != j)
                    {
                        KeyPair pair = new KeyPair();
                        pair.left = words.get(i);
                        pair.right = words.get(j);
                        output.collect(pair, new IntWritable(1));
                        ++count;
                    }
                }
                KeyPair marginal = new KeyPair();
                marginal.left = words.get(i);
                marginal.right = "";
                marginal.marginal = true;
                output.collect(marginal, new IntWritable(count));
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<KeyPair, IntWritable, Text, Text>
    {
        private int totalCounts = 0;

        public void reduce(KeyPair key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException
        {
            if (key.marginal)
            {
                totalCounts = 0;
                while (values.hasNext())
                {
                    totalCounts += values.next().get();
                }
            }
            else
            {
                int sum = 0;
                while (values.hasNext())
                {
                    sum += values.next().get();
                }
                double probability = ((double)sum) / totalCounts;

                String outputPair = key.left + "," + key.right;
                String outputText = sum + "," + probability;
                output.collect(new Text(outputPair), new Text(outputText));
            }
        }
    }

    public static void main(String... args)
    throws IOException
    {
        JobConf conf = new JobConf(SentencePairs.class);
        conf.setJobName("partb-pairs");

        conf.setOutputKeyClass(KeyPair.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(SentenceInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumReduceTasks(8);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        FileOutputFormat.setCompressOutput(conf, false);

        JobClient.runJob(conf);
    }
}


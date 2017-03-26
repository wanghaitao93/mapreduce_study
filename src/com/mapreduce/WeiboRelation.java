package com.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WeiboRelation {

    public static class RelationMapper
            extends Mapper<Object, Text, Text, IntWritable>
    {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String args[] = value.toString().split("\t");

            context.write(new Text(args[0]), new IntWritable(-1));
            context.write(new Text(args[1]), new IntWritable(1));
        }
    }

    public static class RelationCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values,
                        Context context
        ) throws IOException, InterruptedException
        {
            int inDegreeTimes = 0;
            int outDegreeTimes = 0;
            for (IntWritable val : values) {
                if (val.get() > 0)
                {
                    inDegreeTimes += 1;
                }else
                {
                    outDegreeTimes -= 1;
                }
            }

            context.write(key, new IntWritable(inDegreeTimes));
            context.write(key, new IntWritable(outDegreeTimes));
        }
    }

    public static class RelationReducer
            extends Reducer<Text,IntWritable,Text, Text> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int inDegreeTimes = 0;
            int outDegreeTimes = 0;
            for (IntWritable val : values) {
                int value = val.get();
                if (value > 0)
                {
                    inDegreeTimes += value;
                }else
                {
                    outDegreeTimes += Math.abs(value);
                }
            }

            String result = inDegreeTimes + ", " + outDegreeTimes;
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: relation <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "weibo relation");
        job.setJarByClass(WeiboRelation.class);
        job.setMapperClass(RelationMapper.class);
        job.setCombinerClass(RelationCombiner.class);
        job.setReducerClass(RelationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
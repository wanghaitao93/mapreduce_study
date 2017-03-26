package com.mapreduce;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CalculatePI {

    public static class TotalNumMapper
            extends Mapper<Object, Text, Text, IntWritable>
    {

        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
;
        private static Random random = new Random();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            int totalNum = Integer.parseInt(value.toString());
            int index = 0;
            word.set("result");

            while (index < totalNum)
            {
                float x = random.nextFloat();
                float y = random.nextFloat();

                IntWritable intWritable = new IntWritable(0);

                if (Math.sqrt(x*x + y*y) < 1) {
                    intWritable = one;
                }
                context.write(word, intWritable);
                index++;
            }
        }
    }

    public static class CalculatePIReducer
            extends Reducer<Text,IntWritable,Text,FloatWritable> {

        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int hitSum = 0;
            int totalSum = 0;
            for (IntWritable val : values) {
                hitSum += val.get();
                totalSum++;
            }

            result.set((float)hitSum/(float)totalSum*4);
            context.write(new Text(totalSum+""), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: PI <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "PI count");
        job.setJarByClass(CalculatePI.class);
        job.setMapperClass(TotalNumMapper.class);
//        job.setCombinerClass(CalculatePIReducer.class);
        job.setReducerClass(CalculatePIReducer.class);
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
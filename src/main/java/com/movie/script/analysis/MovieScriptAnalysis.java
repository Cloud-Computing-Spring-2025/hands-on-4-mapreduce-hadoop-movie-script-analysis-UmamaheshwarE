package com.movie.script.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieScriptAnalysis {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MovieScriptAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Task 1: Most Frequent Words by Character
        Job job1 = Job.getInstance(conf, "Most Frequent Words by Character");
        job1.setJarByClass(MovieScriptAnalysis.class);
        job1.setMapperClass(CharacterWordMapper.class);
        job1.setReducerClass(CharacterWordReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/task1"));
        if (!job1.waitForCompletion(true)) {
            System.exit(1);  // If job1 fails, exit with status 1
        }

        // Task 2: Dialogue Length Analysis
        Job job2 = Job.getInstance(conf, "Dialogue Length Analysis");
        job2.setJarByClass(MovieScriptAnalysis.class);
        job2.setMapperClass(DialogueLengthMapper.class);
        job2.setReducerClass(DialogueLengthReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/task2"));
        if (!job2.waitForCompletion(true)) {
            System.exit(1);  // If job2 fails, exit with status 1
        }

        // Task 3: Unique Words by Character
        Job job3 = Job.getInstance(conf, "Unique Words by Character");
        job3.setJarByClass(MovieScriptAnalysis.class);
        job3.setMapperClass(UniqueWordsMapper.class);
        job3.setReducerClass(UniqueWordsReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/task3"));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}

package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CharacterWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalWordCount = 0;

        // Iterate over all the values for this key (each value represents a word spoken by the character)
        for (IntWritable val : values) {
            totalWordCount += val.get(); // Add up the count (all values are 1)
        }

        // Set the total word count for the character
        result.set(totalWordCount);

        // Emit the character's name (key) and the total word count (value)
        context.write(key, result);
    }
}

package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DialogueLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalLength = 0;

        // Iterate through all values (which are the lengths of dialogues)
        for (IntWritable val : values) {
            totalLength += val.get(); // Add the length of each dialogue
        }

        // Set the total length of dialogues for the character
        result.set(totalLength);

        // Emit the character's name and the total dialogue length
        context.write(key, result);
    }
}

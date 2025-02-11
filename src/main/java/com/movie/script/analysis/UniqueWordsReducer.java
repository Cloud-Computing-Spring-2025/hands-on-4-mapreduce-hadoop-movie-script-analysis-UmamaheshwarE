package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Create a HashSet to store unique words for the character
        HashSet<String> uniqueWordsSet = new HashSet<>();

        // Iterate through all the values (which are space-separated strings of unique words)
        for (Text val : values) {
            // Split the value into individual words
            String[] words = val.toString().split(" ");
            
            // Add each word to the HashSet (duplicates will be ignored)
            for (String word : words) {
                uniqueWordsSet.add(word);
            }
        }

        // Join the unique words into a single space-separated string
        String uniqueWordsList = String.join(" ", uniqueWordsSet);

        // Set the final list of unique words as the value
        result.set(uniqueWordsList);

        // Emit the character's name (key) and their unique words (value)
        context.write(key, result);
    }
}

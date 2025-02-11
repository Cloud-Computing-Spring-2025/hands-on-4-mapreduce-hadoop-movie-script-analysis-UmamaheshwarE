package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Get the line as a string
        String line = value.toString().trim();

        // Skip empty lines
        if (line.isEmpty()) {
            return;
        }

        // Split the line into the character's name and the dialogue (after the ':')
        String[] parts = line.split(":", 2);
        if (parts.length == 2) {
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            // Set the character's name as the key
            character.set(characterName);

            // Tokenize the dialogue to extract words
            StringTokenizer tokenizer = new StringTokenizer(dialogue, " ,.!?;:\"()[]{}-");
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().toLowerCase(); // Convert to lowercase to handle case insensitivity
                word.set(token);

                // Emit each word with the character's name as key, and 1 as value to count the word occurrences
                context.write(character, one);
            }
        }
    }
}

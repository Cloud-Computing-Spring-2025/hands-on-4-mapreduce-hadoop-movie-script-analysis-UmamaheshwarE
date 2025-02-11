package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        // Skip empty lines
        if (line.isEmpty()) {
            return;
        }

        // Split the line into character's name and dialogue
        String[] parts = line.split(":", 2);
        if (parts.length == 2) {
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            // Get the length of the dialogue (excluding the character's name)
            int dialogueLength = dialogue.length();

            // Set the character's name as key
            character.set(characterName);

            // Set the length of the dialogue as value
            wordCount.set(dialogueLength);

            // Emit the character's name and the length of their dialogue
            context.write(character, wordCount);

            // Use counters to track the total number of characters processed
            context.getCounter("MovieScript", "Total Characters Processed").increment(dialogueLength);
        }
    }
}


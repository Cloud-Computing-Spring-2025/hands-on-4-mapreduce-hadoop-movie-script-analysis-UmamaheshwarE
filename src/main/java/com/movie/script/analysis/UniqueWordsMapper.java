package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text uniqueWords = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Get the line from input text
        String line = value.toString().trim();

        // Skip empty lines
        if (line.isEmpty()) {
            return;
        }

        // Split the line into character's name and dialogue (after the ':')
        String[] parts = line.split(":", 2);
        if (parts.length == 2) {
            String characterName = parts[0].trim();  // Extract character's name
            String dialogue = parts[1].trim();       // Extract the dialogue

            // Create a HashSet to store unique words
            HashSet<String> uniqueWordSet = new HashSet<>();

            // Tokenize the dialogue to extract words
            StringTokenizer tokenizer = new StringTokenizer(dialogue, " ,.!?;:\"()[]{}-");
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().toLowerCase();  // Convert to lowercase
                uniqueWordSet.add(word);  // Add word to the set
            }

            // Set the character's name as the key
            character.set(characterName);

            // Join all unique words into a single string
            uniqueWords.set(String.join(" ", uniqueWordSet));

            // Emit the character's name and their unique words
            context.write(character, uniqueWords);
        }
    }
}

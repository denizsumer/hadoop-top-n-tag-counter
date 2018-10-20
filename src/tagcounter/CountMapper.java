/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tagcounter;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 CountMapper is a mapper class for mapping the key-value pairs 
 @author Deniz Sumer 101527131@student.swin.edu.au
 @version 1.0.1842
 */
public class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static LongWritable ONE = new LongWritable(1);
    private Text word = new Text();

    /**
     Overridden method for map(). It splits the line, takes out the key (10th)
     tab separated element of value, splits it by the commas and maps them with
     the value of 1.

     @param key line number of the text file
     @param value text in respective line
     @param context output context
     @throws IOException
     @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Convert line to string
        String line = value.toString();
        // Split the string, convert 10th elements(tags) to TSV
        String tags = line.split("\t")[10].replace(",", "\t");
        // Split tags
        StringTokenizer tokenizer = new StringTokenizer(tags);
        while (tokenizer.hasMoreTokens()) {
            // convert tag to lowercase and set as Text object
            word.set(tokenizer.nextToken().toLowerCase());
            // Write Text object to key and 1 as value
            context.write(word, ONE);
        }
    }
}

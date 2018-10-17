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
 *
 * @author 101527131
 */
public class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static LongWritable one = new LongWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String tags = line.split("\t")[10].replace(",", " ");
        StringTokenizer tokenizer = new StringTokenizer(tags);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken().toLowerCase());
            context.write(word, one);
        }
    }
}

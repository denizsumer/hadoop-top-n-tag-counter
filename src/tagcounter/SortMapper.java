/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tagcounter;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**

 @author Deniz
 */
public class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] e = line.split("\t");
        context.write(new IntWritable(Integer.parseInt(e[1])), new Text(e[0]));
    }
}

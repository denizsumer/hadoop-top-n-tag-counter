/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tagcounter;


import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 *
 * @author 101527131
 */
public class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) 
    throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
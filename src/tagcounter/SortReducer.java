/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tagcounter;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**

 @author Deniz
 */
public class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    int n = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        n = 0;
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) {
        if (n < 10) {
            try {
                for (Text value : values) {
                    context.write(key, value);
                    n++;
                }
            }
            catch (Exception e) {
            }
        }
    }
}

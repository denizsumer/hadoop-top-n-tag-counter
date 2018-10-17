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
public class SortMapper extends Mapper<Text, IntWritable, IntWritable, Text> {

    @Override
    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        context.write(value, key);
    }
}

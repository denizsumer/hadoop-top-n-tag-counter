package tagcounter;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 SortMapper is a mapper class for swapping the key value pairs

 @author Deniz Sumer 101527131@student.swin.edu.au
 @version 1.0.1842
 */
public class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    /**
     Overridden method for map(). It splits the line, takes out the key (10th)
     tab separated element of value, splits it by the commas and maps them with
     the value of 1.

     @param key line number of the text file
     @param value key value pair in TSV format
     @param context output context
     @throws IOException
     @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Convert value to string
        String line = value.toString();
        //Split string as [0] key and [1] as value
        String[] e = line.split("\t");
        //Swap the key value pairs and write to context
        context.write(new IntWritable(Integer.parseInt(e[1])), new Text(e[0]));
    }
}

package tagcounter;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 SortReducer is a reducer class to filter the the key-value pairs.

 @author Deniz Sumer 101527131@student.swin.edu.au
 @version 1.0.1842
 */
public class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    int n = 0;

    /**
     Overridden method for setup(). Sets the n number

     @param context output context
     @throws IOException
     @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        n = 0;
    }

    /**
     Overridden method for reduce(). Filters top ten values.

     @param key count of tag
     @param values tag - always expected 1 value from CountMapper context
     @param context output context
     @throws IOException
     @throws InterruptedException
     */
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Run for the first 10 keys
        if (n < 10) {
            for (Text value : values) {
                // Write key value pairs to context
                context.write(key, value);
                // Increase the counter
                n++;
            }
        }
    }
}

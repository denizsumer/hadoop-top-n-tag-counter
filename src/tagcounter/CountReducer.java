package tagcounter;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 CountReducer is a reducer class to sum the key-value pairs

 @author Deniz Sumer 101527131@student.swin.edu.au
 @version 1.0.1842
 */
public class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     Overridden method for reduce(). Sums values and writes to the output
     context.

     @param key tag
     @param values count of a tag - always expected 1 from CountMapper context
     @param context output context
     @throws IOException
     @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable val : values) {
            // Sum the values
            sum += val.get();
        }
        // Write Text object to key and counted sum as value
        context.write(key, new LongWritable(sum));
    }
}

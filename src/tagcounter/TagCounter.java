package tagcounter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 TagCounter is a MapReduce function in EMR to analyze the full dataset and
 determine the 10 most common user tags for media resources in the meta‐data.
 Full dataset can be found in text file in s3://cos80001-emr/yfcc100m

 @author Deniz Sumer 101527131@student.swin.edu.au
 @version 1.0.1842
 */
public class TagCounter {

    /**
     This is the main method. Example for args: s3://my-bucket/input/
     s3://my-bucket/output/

     @param args input and output folders
     @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //count the tags
        if (!count(args)) {
            System.exit(1);
        }
        //sort by tags and filter top ten
        if (!sort(args)) {
            System.exit(1);
        }
    }

    /**
     This method creates a Job for parsing the data and counting the tags.

     @param args input and output folders
     @return boolean completion of Job
     @throws Exception
     */
    public static boolean count(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job countJob = new Job(conf, "TagCount");
        countJob.setJarByClass(TagCounter.class);
        countJob.setMapperClass(CountMapper.class);
        countJob.setReducerClass(CountReducer.class);
        //Number of reducers
        countJob.setNumReduceTasks(10);
        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(LongWritable.class);
        countJob.setInputFormatClass(TextInputFormat.class);
        countJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(countJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(countJob, new Path(args[1]));
        return countJob.waitForCompletion(true);
    }

    /**
     This method creates a Job for swapping keys and values, sorting and
     filtering top 10 tags

     @param args input and output folders
     @return boolean completion of Job
     @throws Exception
     */
    public static boolean sort(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job sortJob = new Job(conf, "TagSort");
        sortJob.setJarByClass(TagCounter.class);
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setReducerClass(SortReducer.class);
        sortJob.setNumReduceTasks(1);
        sortJob.setSortComparatorClass(IntDescComparator.class);
        sortJob.setOutputKeyClass(IntWritable.class);
        sortJob.setOutputValueClass(Text.class);
        sortJob.setInputFormatClass(TextInputFormat.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(sortJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(sortJob, new Path(args[1], "sorted"));
        return sortJob.waitForCompletion(true);
    }

    /**
     Overridden class for descending sort of IntWritable objects.
     */
    public static class IntDescComparator extends WritableComparator {

        /**
         Default constructor calls the super class
         */
        public IntDescComparator() {
            super(IntWritable.class);
        }

        /**
         Overridden compare() method for descending sort of IntWritable objects.
         * @param b1
         * @param s1
         * @param l1
         * @param b2
         * @param s2
         * @param l2
         * @return int 1, 0 or -1 depending on the comparison result
         */
        @Override
        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            int thisValue = readInt(b1, s1);
            int thatValue = readInt(b2, s2);
            return (thisValue < thatValue ? 1 : (thisValue == thatValue ? 0 : -1));
        }
    }
}

/*
s3://101527131-emr/input/
s3://101527131-emr/output4/
 */

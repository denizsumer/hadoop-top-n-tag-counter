/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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

 @author 101527131
 */
public class TagCounter {

    public static void main(String[] args) throws Exception {
        if (!count(args)) {
            System.exit(1);
        }
        if (!sort(args)) {
            System.exit(1);
        }
    }

    public static boolean count(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job countJob = new Job(conf, "TagCount");
        countJob.setJarByClass(TagCounter.class);
        countJob.setMapperClass(CountMapper.class);
        countJob.setReducerClass(CountReducer.class);
        countJob.setNumReduceTasks(10);
        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(LongWritable.class);
        countJob.setInputFormatClass(TextInputFormat.class);
        countJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(countJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(countJob, new Path(args[1]));
        return countJob.waitForCompletion(true);
    }

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

    public static class IntDescComparator extends WritableComparator {

        public IntDescComparator() {
            super(IntWritable.class);
        }

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

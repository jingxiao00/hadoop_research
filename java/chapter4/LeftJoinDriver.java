
package LeftOutJoin_hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.umd.cloud9.io.pair.PairOfStrings;

public class LeftJoinDriver {
    public static void main( String[] args ) throws Exception {
        Path transactions =    new Path("input/transactions.txt");// input
        Path users =  new Path("input/users.txt");        // input
        Path output = new Path("output/1");        // output

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(LeftJoinDriver.class);
        job.setJobName("Phase-1: Left Outer Join");

        job.setPartitionerClass(SecondarySortPartitioner.class);

        job.setGroupingComparatorClass(SecondarySortGroupComparator.class);

        job.setSortComparatorClass(PairOfStrings.Comparator.class);

        job.setReducerClass(LeftJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        MultipleInputs.addInputPath(job, transactions, TextInputFormat.class, LeftJoinTransactionMapper.class);
        MultipleInputs.addInputPath(job, users, TextInputFormat.class, LeftJoinUserMapper.class);

        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);
        FileOutputFormat.setOutputPath(job, output);
        if (job.waitForCompletion(true)) {
            return;
        } else {
            throw new Exception("Phase-1: Left Outer Join Job Failed");
        }
    }
}

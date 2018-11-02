
package LeftOutJoin_hadoop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;


public class LocationCountMapper 
    extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
        System.out.println(value);
        String [] tokens = value.toString().split("\t");
        System.out.println("tokens:" + tokens.length);
                                            
        outputKey.set(tokens[0]);
        outputValue.set( tokens[1]);
        context.write(outputKey,outputValue);
    }
}




import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class LeftJoinTransactionMapper 
    extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws java.io.IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(), "\t");
        System.out.println("tokens size:" + tokens.length);

        String productID = tokens[1];
        String userID = tokens[2];
        outputKey.set(userID, "2");
        outputValue.set("P", productID);
        context.write(outputKey, outputValue);
    }
}


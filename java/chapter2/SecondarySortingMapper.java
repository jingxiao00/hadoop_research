

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondarySortingMapper extends
        Mapper<LongWritable, Text, DateTemperaturePair, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String yearMonth = tokens[0] + "-" + tokens[1];
        String day = tokens[2];
        int temperature = Integer.parseInt(tokens[3]);
        DateTemperaturePair reduceKey = new DateTemperaturePair();
        reduceKey.setYearMonth(yearMonth);
        reduceKey.setDay(day);
        reduceKey.setTemperature(temperature);
        context.write(reduceKey, new IntWritable(temperature));
    }
}



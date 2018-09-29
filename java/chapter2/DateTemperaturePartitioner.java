
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DataTemperaturePartitioner
    extends Partitioner<DateTemperaturePair, Text> {
    @Override
    public int getPartition(DateTemperaturePair pair, Text text,
        int numberOfPartitions) {
        return Math.abs(pair.getYearMonth().hashCode() % numberOfPartitions);
    }
}

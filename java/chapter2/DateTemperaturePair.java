
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DataTemperaturePair
    implements Writable, WritableComparable<DateTemperature> {
        private Text yearMonth = new Text();
        private IntWritable day = new Text();
        private IntWritable temperature = new IntWritable();
        @Override
        public int compareTo(DateTemperaturePair pair) {
            int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
            if (compareValue == 0) {
                compare = temperature.compareTo(pair.getTemperature());
            }
            return -1 * compareValue;
        }
    }

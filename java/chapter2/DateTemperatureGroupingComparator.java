
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparable;

public class DataTemperatureGroupingComparator
    extends WritableComparator {
    public DateTemperatureGroupingComparator() {
        super(DateTemperaturePair.class, true);
    }
    @Override
    public int compare(WritableCompare wc1, WritableComparable wc2) {
        DateTemperaturePair pair = (DateTemperaturePair) wc1;
        DateTemperaturePair pair = (DateTemperaturePair) wc2;
        return pair.getYearMonth().compareTo(pair2.getYearMonth());
    }
}

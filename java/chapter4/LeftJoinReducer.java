
package LeftOutJoin_hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.umd.cloud9.io.pair.PairOfStrings;
import java.util.Iterator;


public class LeftJoinReducer 
    extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {
    Text productID = new Text();
    Text locationID = new Text("undefined");
    @Override
    public void reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context) 
        throws java.io.IOException, InterruptedException {
        System.out.println("key=" + key);
        Iterator<PairOfStrings> iterator = values.iterator();
        System.out.println("values");
        if (iterator.hasNext()) {
            PairOfStrings firstPair = iterator.next();
            System.out.println("firstPair="+firstPair.toString());
            if (firstPair.getLeftElement().equals("L")) {
                locationID.set(firstPair.getRightElement());
            }
        }

        while (iterator.hasNext()) {
            PairOfStrings productPair = iterator.next(); 
            System.out.println("productPair="+productPair.toString());
            productID.set(productPair.getRightElement());
            context.write(productID, locationID);
        }
    }
}

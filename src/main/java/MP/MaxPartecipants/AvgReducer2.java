package MP.MaxPartecipants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class AvgReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce (Text text, Iterable<IntWritable> iterable, Context context) throws IOException, InterruptedException {

        float avgValue = 0;
        float sum = 0;
        int count = 0;

        for (IntWritable it : iterable) {
        	sum += it.get();
        	count++;
        }
        
        avgValue = (sum/ count);

        context.write(text, new IntWritable((int) avgValue));
    }
}

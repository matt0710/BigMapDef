package MP.MaxPartecipants;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class AvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce (Text text, Iterable<FloatWritable> iterable, Context context) throws IOException, InterruptedException {

        float avgValue = 0;
        float sum = 0;
        float count = 0;

        for (FloatWritable it : iterable) {
        	sum += it.get();
        	count++;
        }
        
        avgValue = (sum/ count);

        context.write(text, new FloatWritable(avgValue));
    }
}

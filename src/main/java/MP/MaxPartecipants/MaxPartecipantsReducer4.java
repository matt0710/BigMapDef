package MP.MaxPartecipants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class MaxPartecipantsReducer4 extends Reducer<Text, Text, Text, Text> {
	
	

    public void reduce (Text text, Iterable<Text> iterable, Context context) throws IOException, InterruptedException {

        String[] row = new String[2];
        HashMap<String, Integer> map = new HashMap<>();
        int singleMax = 0;
        
        for (Text t : iterable) {
            row = t.toString().split(";");
            if (map.containsKey(row[0])) {
            		map.replace(row[0], Integer.parseInt(row[1]) + map.get(row[0]));
            }
            else {
            map.put(row[0], Integer.parseInt(row[1]));
            }
        }
        Integer max = Collections.max(map.values());
        
        for (Map.Entry<String, Integer> entry: map.entrySet())
        {
            if (max == entry.getValue()) {
                String receivingMax = entry.getKey();
                context.write(text, new Text(receivingMax));
            }
        }
          
    }
    
}
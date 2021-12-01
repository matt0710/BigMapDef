package MP.MaxPartecipants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.*;

public class EducationReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	HashMap<String, Integer> map ;//= new HashMap<>();
	public void setup (Context context) {
		map = new HashMap<>();
	}

    public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    
    	for (IntWritable val: values) {
    		if (map.containsKey(key.toString())) {
    			map.replace(key.toString(), val.get() + map.get(key.toString()));
    		}
    		else {
    		map.put(key.toString(), val.get());
    		}
    	}
    }
    
    public void cleanup (Context context) throws IOException, InterruptedException {
    	
    	List<Integer> partecipants = new ArrayList<>(map.values());
    	Collections.sort(partecipants, Collections.reverseOrder());
    	
    	if (partecipants.size() < 5)
    		for (int i=0; i<partecipants.size(); i++) {
				for (Map.Entry<String,Integer> entry : map.entrySet()) {
					if (partecipants.get(i) == entry.getValue()) {
						context.write(new Text(entry.getKey()), new IntWritable(partecipants.get(i)));
						}
					}
				}
    	else
    		for (int i=0; i<5; i++) {
    				for (Map.Entry<String,Integer> entry : map.entrySet()) {
    					if (partecipants.get(i) == entry.getValue()) {
    						context.write(new Text(entry.getKey()), new IntWritable(partecipants.get(i)));
    						}
    					}
    				}
    		
    			}
}
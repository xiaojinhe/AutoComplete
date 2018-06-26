import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		int threshold;

		@Override
		public void setup(Context context) {
		    Configuration conf = context.getConfiguration();
		    threshold = conf.getInt("threshold", 20);
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (value == null || value.toString().trim().length() == 0) {
				return;
			}

			String line = value.toString().trim();

			String[] wordsPlusCount = line.split("\t");
			if (wordsPlusCount.length < 2) {
				return;
			}

			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			if (count < threshold) {
				return;
			}

			StringBuilder stringBuilder = new StringBuilder();
			for (int i = 0; i < words.length - 1; i++) {
				stringBuilder.append(words[i]).append(" ");
			}
			String outputKey = stringBuilder.toString().trim();
			String outputValue = words[words.length - 1] + "=" + count;

			if (outputKey != null && outputKey.length() >= 1) {
				context.write(new Text(outputKey), new Text(outputValue));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 10);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			PriorityQueue<Value> pq = new PriorityQueue<Value>();

			for (Text value : values) {
                pq.offer(new Value(value));
                if (pq.size() > n) {
                    pq.poll();
                }
            }

			while (!pq.isEmpty()) {
			    Value value = pq.poll();
			    context.write(new DBOutputWritable(key.toString(), value.word, value.count), NullWritable.get());
            }

            /*TreeMap<Integer, List<String>> treeMap = new TreeMap<Integer, List<String>>(Collections.reverseOrder());

			for (Text value : values) {
			    String[] wordAndCount = value.toString().trim().split("=");
			    String word = wordAndCount[0].trim();
			    int count = Integer.parseInt(wordAndCount[1].trim());
			    if (treeMap.containsKey(count)) {
			    	treeMap.get(count).add(word);
				} else {
			    	List<String> list = new ArrayList<String>();
			    	list.add(word);
			    	treeMap.put(count, list);
				}
            }

            Iterator<Integer> iterator = treeMap.keySet().iterator();

			for (int i = 0; iterator.hasNext() && i < n;) {
			    int keyCount = iterator.next();
			    List<String> words = treeMap.get(keyCount);
			    for (String w : words) {
			        context.write(new DBOutputWritable(key.toString(), w, keyCount), NullWritable.get());
			        i++;
                }
            }*/
		}
	}

	static class Value implements Comparable<Value>{
	    private String word;
	    private int count;

	    Value(Text value) {
            String[] wordPlusCount = value.toString().trim().split("=");
            this.word = wordPlusCount[0].trim();
            this.count = Integer.valueOf(wordPlusCount[1].trim());
        }

        @Override
        public int compareTo(Value o) {
            return Integer.compare(this.count, o.count);
        }
    }
}

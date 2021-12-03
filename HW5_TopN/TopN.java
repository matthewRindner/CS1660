import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import java.util.*; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopN {
	
	
	public static class TopNMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	    private Hashset<String> stopWords = new HashSet<>(Arrays.asList("he", "she",
     	"they", "the", "a", "an", "are", "you", "of", "is", "and", "or"));

	    private static Map<String, Integer> map = new HashMap<String, Integer>();
	    private static PriorityQueue<Entry<String, Integer>> pq;
	    
	    @Override
	    public void setup(Context context) throws IOException, InterruptedException {
	    	pq = new PriorityQueue<Map.Entry<String, Integer>>(new Comparator<Map.Entry<String, Integer>>(){
	    		@Override
	    		public int compare(Map.Entry<String, Integer> one, Map.Entry<String, Integer> two) {
	    			if(one.getValue() == two.getValue()) {
	    				return one.getKey().compareTo(two.getKey());
	    			} else {
	    				return one.getValue() - two.getValue();
	    			}
	    		}
	    	});
	    }
	    
	    @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	//data cleaning
	    	String line = value.toString().replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
	    	
	    	StringTokenizer tokenizer = new StringTokenizer(line);
	    	
	    	while(tokenizer.hasMoreTokens()) {
	    		String token = tokenizer.nextToken();
	    		if(stopList.contains(token)) {
	    			continue;
	    		}
	    		if(!map.containsKey(token)) {
	    			map.put(token, 1);
	    		} else {
	    			Integer temp = map.get(token) + 1;
	    			map.replace(token, temp);
	    		}
	    	}
	    	
	    	Iterator<Map.Entry<String, Integer>> mapIterator = map.entrySet().iterator();
	    	
	    	while(mapIterator.hasNext()) {
	    		pq.add(mapIterator.next());
	    		if(pq.size() > 5) {
	    			pq.remove();
	    		}
	    	}
	    }
	    	
	    @Override
	    public void cleanup(Context context) throws IOException, InterruptedException {
	    	while(!pq.isEmpty()) {
	    		Map.Entry<String, Integer> entry = pq.remove();
	    		context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
	    	}
		}
	}




  public static class TopNReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
	  private static PriorityQueue<Map.Entry<String, Integer>> pq;
    
	  @Override
	  public void setup(Context context) throws IOException, InterruptedException {
		  pq = new PriorityQueue<Map.Entry<String, Integer>>(new Comparator<Map.Entry<String, Integer>>(){
	    		@Override
	    		public int compare(Map.Entry<String, Integer> one, Map.Entry<String, Integer> two) {
	    			if(one.getValue() == two.getValue()) {
	    				return one.getKey().compareTo(b.getKey());
	    			} else {
	    				return one.getValue() - two.getValue();
	    			}
	    		}
	    	});
	  }
	  
	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		  Integer sum = 0;
		  for(IntWritable val: values) {
			  sum += val.get();
		  }
		 
		  Map.Entry<String, Integer> entry = new AbstractMap.SimpleEntry<>(key.toString(), sum);
		  pq.add(entry);
		  //keeps only top 5 most used words
		  if(pq.size() > 5) {
			  pq.remove();
		  }	  
	  }
	  
	  @Override
	  public void cleanup(Context context) throws IOException, InterruptedException {
		  while(!pq.isEmpty()) {
	    		Map.Entry<String, Integer> entry = pq.remove();
	    		context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
		  }
	  }
  }





  public static void main(String[] args) throws Exception {


	  if(args.length != 2) {
			 System.err.println("There are not excatly 2 args");
			 System.exit(-1);
		}

	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Top N");
	    job.setJarByClass(TopN.class);

	    job.setMapperClass(TopNMapper.class);
	    job.setReducerClass(TopNReducer.class);

	    job.setNumReduceTasks(1);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

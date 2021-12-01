import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import java.util.*; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;   //might need if use LongWritable over Object
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopN {

  public static class Top_N_Mapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Hashset<String> stopWords = new HashSet<>(Arrays.asList("he", "she",
     "they", "the", "a", "an", "are", "you", "of", "is", "and", "or"));

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //tockinize input on whitespace
      StringTokenizer itr = new StringTokenizer(value.toString());
      //loop per tocines on a line
      while (itr.hasMoreTokens()) 
      {
        word.set(itr.nextToken());
        if(word.toString() != !stopWords.contains(word.toString()))
          context.write(word, new IntWritable(fileName));
      }
    }
  }


  public static class Top_N_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);

    }
  }

  public static void main(String[] args) throws Exception {

    if(args.length != 2)
    {
      System.err.println("There are not 2 exactly 2 input args");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "top 5");
    job.setJarByClass(TopN.class);

    job.setMapperClass(Top_N_Mapper.class);
    //job.setCombinerClass(Top_N_Reducer.class);
    job.setReducerClass(Top_N_Reducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
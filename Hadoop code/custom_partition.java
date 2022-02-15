import java.io.IOException;
import java.util.*; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class custom_partition {
  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
 {
 
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
            context.write(new Text(line[11]),new IntWritable(Integer.parseInt(line[3])));
}  
} 
public static class dpart extends Partitioner<Text,IntWritable>
{
public int getPartition(Text key,IntWritable value,int nr)
{
 if(value.get()<5)
	  return 0;
 if(value.get()<10)
 	  return 1;
  else
	  return 2;
  } 
}


public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
{
int s = 0;
public  IntWritable r = new IntWritable();
public Text word  = new Text("Total count:");
public void reduce(Text key, Iterable<IntWritable> values, Context context) 
throws IOException, InterruptedException {	        
    for (IntWritable val : values) {
    	s=s+1;
    	context.write(key, val);
    }
}

public void cleanup(Context context) throws IOException, InterruptedException
{
	r.set(s);
    context.write(word,r);  
  }
}

public static void main(String[] args) throws Exception 
{
Configuration conf = new Configuration();
Job job = new Job(conf, "Wine Quality Prediction");
job.setJarByClass(custom_partition.class);
job.setPartitionerClass(dpart.class);
job.setNumReduceTasks(3);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setInputFormatClass(TextInputFormat.class);	    
job.setOutputFormatClass(TextOutputFormat.class);    
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
}        

}
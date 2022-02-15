//average
import java.io.IOException; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class alcohol{
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	 {
	 //private final static IntWritable one = new IntWritable(1);
	 IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] line = value.toString().split(",");
	        	int r=Integer.parseInt(line[5]); //free sulphur dioxide
	        	if(r>=36)
	        	{
	        		try
	        		{
	        			context.write(new Text(line[12]), new IntWritable(Integer.parseInt(line[10])));// taste & quality values
	        		}
	        		catch(Exception e)
	        		{
	        			context.write(new Text(line[12]), new IntWritable(Integer.parseInt(line[10])));
	        		}
	        	}
	        	
	        }  
	     } 

	  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		  int max_s=0,max=0;
		  Text max_key=new Text("According to pH Range Average Quality of the wine in Red color: ");
		  Text m_key=new Text();
		    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		    throws IOException, InterruptedException {	        
		    	int s=0,c=0,avg=0;
		        for (IntWritable val : values) {
		        	c+=1;
		            s+= val.get();
		        }
		        avg=s/c;
		        if(max<avg)
		        {
		        	max=avg;
		        	m_key.set(key);
		        }
		  context.write(key, new IntWritable(avg));
		  
		   }
		    @Override
		    public void cleanup(Context context) throws IOException, InterruptedException 
		    {
		    context.write(max_key, new IntWritable(max));
            }
		  }
	  public static void main(String[] args) throws Exception
	  {
		   Configuration conf = new Configuration();
		   Job job = new Job(conf, "avg");
		   job.setJarByClass(alcohol.class);
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
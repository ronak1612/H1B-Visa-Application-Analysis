/*
0 s_no                	int                 	                    
1 case_status         	string              	                    
2 employer_name       	string              	                    
3 soc_name            	string              	                    
4 job_title           	string              	                    
5 full_time_position  	string              	                    
6 prevailing_wage     	bigint              	                    
7 year                	string              	                    
8 worksite            	string              	                    
9 longitute           	double              	                    
10 latitute            	double */    
  //b) find top 5 locations in the US who have got certified visa for each year.[certified]
//
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





public class WorksiteTop5DataEngineer {
	public static class WorkSiteMapper extends Mapper < LongWritable, Text, Text, LongWritable > 
	{
	    
	    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
	    {
	            String[] records = values.toString().split("\t");
	            String worksite= records[8].toUpperCase();
	            String year= records[7];
	          //  String job_title= records[4].toUpperCase();
	            String case_status=records[1].toUpperCase();
	            
	          //b) find top 5 locations in the US who have got certified visa for each year.[certified]
	            
	   if (case_status.equals("CERTIFIED")) 
	                  {
	                Text workyear = new Text(worksite+"\t"+year);
	                context.write(workyear, new LongWritable(1));
	            
	                  }

	    }       
   }//mapper's end
	
	
	public static class YearPartitioner extends Partitioner < Text, LongWritable > 
	{
	    @Override
	    public int getPartition(Text key, LongWritable value, int numReduceTasks) 
	    {
	        String[] workyear = key.toString().split("\t");
	        String year= workyear[1];
	        
	        if (year.equals("2011"))
	            return 0;
	        else if (year.equals("2012"))
	            return 1;
	        else if (year.equals("2013"))
	            return 2;
	        else if (year.equals("2014"))
	            return 3;
	        else if (year.equals("2015"))
	            return 4;
	        else if (year.equals("2016"))
	            return 5;
	        else
	            return 6;
	    }
	}// partitioner end
	
	public static class WorksiteReducer extends Reducer<Text,LongWritable,NullWritable,Text>
	{
		private TreeMap<LongWritable, Text> tm = new TreeMap<LongWritable, Text>();
		long totalApp=0;
		public void reduce(Text workyear,Iterable <LongWritable> values,Context context) throws IOException, InterruptedException
		{
			        totalApp=0;
			 
					for(LongWritable x:values)
					{
					totalApp+=x.get();
					}
					
			tm.put(new LongWritable(totalApp),new Text(workyear+","+totalApp));
			if (tm.size()>5)
				tm.remove(tm.firstKey());
		}
				protected void cleanup(Context context)throws IOException, InterruptedException
				{
					for (Text t : tm.descendingMap().values()) 
						context.write(NullWritable.get(), t);
				}				
				} // reducer end	


public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
{
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Top  5 worksite");

job.setJarByClass(WorksiteTop5DataEngineer.class);
job.setMapperClass(WorkSiteMapper.class);
job.setPartitionerClass(YearPartitioner.class);
job.setReducerClass(WorksiteReducer.class);

job.setNumReduceTasks(7);

job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(LongWritable.class);

job.setOutputKeyClass(NullWritable.class);
job.setOutputValueClass(Text.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);

   }
}

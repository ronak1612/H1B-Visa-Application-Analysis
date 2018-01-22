/*3)Which industry(SOC_NAME) has the most number of Data Scientist positions?
			[certified]

			/*0	s_no                	int                 	                    
			1	case_status         	string              	                    
			2	employer_name       	string              	                    
			3	soc_name            	string              	                    
			4	job_title           	string              	                    
			5	full_time_position  	string              	                    
			6	prevailing_wage     	bigint              	                    
			7	year                	string              	                    
			8	worksite            	string              	                    
			9	longitute           	double              	                    
			10	latitute            	double    */


import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Q3Top10SocName {
	

				public static class SocNameMapper extends Mapper<LongWritable, Text, Text, LongWritable>
				    {

				      public void map(LongWritable offset, Text values, Context writer ) throws IOException, InterruptedException 
					    {

						 String records[]= values.toString().split("\t");
						 String case_status = records[1];  //fetching case_status
					         String job_title= records[4].toUpperCase(); //fetching job_title
					         String soc_name=records[3].toUpperCase(); //fetching soc_name
					               if( soc_name!="NA" && case_status.equals("CERTIFIED") && job_title.contains("DATA SCIENTIST"))
							  {       
							      writer.write( new Text(soc_name+"DATA SCIENTIST"), new LongWritable(1));
						          }
					 }
				    }
			  
			  public static class Top10SocReducer  extends Reducer<Text,LongWritable,NullWritable,Text>
			      {			
					private TreeMap<LongWritable, Text> tm = new TreeMap<LongWritable, Text>();
					long totalApp=0;

					public void reduce(Text socjobtitle,Iterable <LongWritable> values,Context context) throws IOException, InterruptedException
					{
						        totalApp=0;
								for(LongWritable x:values)
								{
								totalApp+=x.get();
								}
								
						tm.put(new LongWritable(totalApp),new Text(socjobtitle+","+totalApp));
						if (tm.size()>10)
							tm.remove(tm.firstKey());
					}

					protected void cleanup(Context context)throws IOException, InterruptedException
							{
								for (Text t : tm.descendingMap().values()) 
									context.write(NullWritable.get(), t);
							}				
				} // reducer end	
			   		
			   		
			      

				  public static void main(String[] args) throws Exception
				 {
				    Configuration conf = new Configuration();
				    Job job = Job.getInstance(conf, "Soc Name Data Scientist");
				    job.setJarByClass(Q3Top10SocName.class);

				    job.setMapperClass(SocNameMapper.class);
				    job.setReducerClass(Top10SocReducer.class);

				    job.setMapOutputKeyClass(Text.class);
				    job.setMapOutputValueClass(LongWritable.class);

				    job.setOutputKeyClass(NullWritable.class);
				    job.setOutputValueClass(Text.class);

				    FileInputFormat.addInputPath(job, new Path(args[0]));
				    FileOutputFormat.setOutputPath(job, new Path(args[1]));
				    System.exit(job.waitForCompletion(true) ? 0 : 1);
				  }
			    }
				



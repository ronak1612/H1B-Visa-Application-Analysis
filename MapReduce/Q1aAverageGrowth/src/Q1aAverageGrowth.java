import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Q1aAverageGrowth
{
	public static class YearMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
    {

      public void map(LongWritable offset, Text values, Context writer ) throws IOException, InterruptedException 
	    {

		 String records[]= values.toString().split("\t");
		 String year = records[7];  //fetching year
                 String job_title= records[4]; //fetching job_title
                       if(job_title.contains("DATA ENGINEER"))
			  {       
		              writer.write( new Text(year+","+"DATA ENGINEER"), new DoubleWritable(1));
	                  }
         }
    }
  
  public static class CountReducer1 extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
      {                 
                      
		Double totalApplication[]= new Double[6];
                int i=0;
		   		
   		 public void reduce(Text year, Iterable<DoubleWritable> values, Context writer) throws IOException, InterruptedException 
	              {
   			 
			      Double count =0.0;
                              
			      for (DoubleWritable x : values)
			               {
					 count+= x.get();
				            }
                  
                             totalApplication[i]=count;
                             i++;
    			}

                  // Now in totalaAplication array we have total number of application for each year.
                 

		public void cleanup(Context writer) throws IOException, InterruptedException 
		    { 
		    for (int i=0;i<6;i++)     
			if (i==0) 
			writer.write(new Text("year"+i), new DoubleWritable(0)); 
			else 
	           writer.write(new Text("year"+i), new DoubleWritable((totalApplication[i]-totalApplication[i-1])*100/totalApplication[i-1])); 
		     
		    } 
      }

  public static void main(String[] args) throws Exception
 {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Growth Percent ");
    job.setJarByClass(Q1aAverageGrowth.class);
    job.setMapperClass(YearMapper.class);
    //job.setCombinerClass(YearMapper.class);
    job.setReducerClass(CountReducer1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
    }
	

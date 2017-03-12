

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


/**
 *
 * @author swarna
 */
public class DriverClass{
/**
 *
 * @author swarna
 */
    
    public static void  main(String arg0[]) throws IOException, InterruptedException, ClassNotFoundException{
  
		
		Configuration configuration  = new Configuration();
		
		Job job1 = Job.getInstance(configuration);
		job1.setJarByClass(DriverClass.class);
                job1.setJobName("InputFiles");
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputValueClass(Text.class);  
		job1.setOutputKeyClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(arg0[0])); //your input path
   		FileOutputFormat.setOutputPath(job1, new Path(arg0[0]+"inter15")); //your intermediate path
		
   		job1.waitForCompletion(true);
                 
                Job job2 = Job.getInstance(configuration);
		job2.setJarByClass(DriverClass.class);
                job2.setJobName("MergeFiles");
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
                job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputValueClass(Text.class);  
		job2.setOutputKeyClass(Text.class);
		
		FileInputFormat.addInputPath(job2,new Path(arg0[0]+"inter15")); //your intermediate path
   		FileOutputFormat.setOutputPath(job2, new Path(arg0[0]+"inter16")); //your intermediate path
		
   		job2.waitForCompletion(true);
                 
                Job job3 = Job.getInstance(configuration);
		job3.setJarByClass(DriverClass.class);
                job3.setJobName("MoviePairs");
		job3.setMapperClass(Map3.class);
		job3.setReducerClass(Reduce3.class);
                job3.setInputFormatClass(KeyValueTextInputFormat.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputValueClass(Text.class);  
		job3.setOutputKeyClass(Text.class);
                job3.setNumReduceTasks(1);
               // configuration.set("mapreduce.output.textoutputformat.separator",":");
                //job3.setOutputFormat(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job3,new Path(arg0[0]+"inter16")); //your intermediate path
   		FileOutputFormat.setOutputPath(job3, new Path(arg0[1])); //your output path
		
   		job3.waitForCompletion(true); 
                        
                Recommender.TopKMovies(arg0[1]);

}
}


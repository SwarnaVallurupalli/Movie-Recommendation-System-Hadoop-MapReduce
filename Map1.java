
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



/**
 *
 * @author swarna
 */
public class Map1 extends Mapper<Object,Object,Text, Text> {
    
     private String Flagtag_moviename="MN~";
     private String Flagtag_userid="U~";
     
        protected void map(Object key, Object value, Mapper<Object,Object,Text, Text>.Context context)
			throws IOException, InterruptedException {
				
	FileSplit inputSplit =  (FileSplit) context.getInputSplit();
	String path = inputSplit.getPath().toString();
	int one=1;
	
	if(path.contains("movies")){
		
		String[] split = value.toString().split(",");		
		String movieid = split[0];
		String moviename=split[1];
		               
		context.write(new Text(movieid),new Text(Flagtag_moviename+moviename));
	}
	if(path.contains("ratings")) {
		
		String[] split = value.toString().split(",");		
		String movieid = split[1];
		String userid=split[0];
		String rating=split[2];
				               
		context.write(new Text(movieid), new Text(Flagtag_userid+userid+","+rating));
	}	
}	
}   
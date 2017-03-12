
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 *
 * @author swarna
 */
public class Reduce2 extends Reducer<Text,Text,Text,Text>
{
protected void reduce(Text key,Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context)
			throws IOException, InterruptedException {
		
                StringBuilder allUserMovieRates = new StringBuilder();
                
                for(Text val:values)
                {
			allUserMovieRates.append(val+"~");
		}
		
		context.write(key, new Text(allUserMovieRates.toString().trim()));
	}
}

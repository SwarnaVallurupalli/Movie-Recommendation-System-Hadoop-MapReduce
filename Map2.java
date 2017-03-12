
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 *
 * @author swarna
 */
public class Map2 extends Mapper<Object,Object,Text,Text> {
   
    protected void map(Object key,Object value, Mapper<Object,Object,Text, Text>.Context context)
			throws IOException, InterruptedException {
				
		
		String[] user_rating_pair = value.toString().split("%");
                String[] current_movie_string=value.toString().split("MN~");
                String[] cm=current_movie_string[1].toString().split("%");
                String current_movie_name=cm[0];
                
                for(int i = 0 ; i < user_rating_pair.length; i++){
                String[] splitvalues= user_rating_pair[i].split("~");
                
                if(splitvalues[0].equals("MN"))
                {
                    String movie_name=splitvalues[1];
                }
                  else if(splitvalues[0].equals("U"))
                  {
                      String[] user_rate_pair=splitvalues[1].toString().split(",");
                      String user_id=user_rate_pair[0];
                      String rating=user_rate_pair[1];
                      context.write(new Text(user_id),new Text(current_movie_name+"%%"+rating));
                  }
                
                }
        }
}
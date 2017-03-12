
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 *
 * @author swarna
 */
public class Map3 extends Mapper<Object,Object,Text,Text> {
   
    protected void map(Object key,Object value, Mapper<Object,Object,Text, Text>.Context context)
			throws IOException, InterruptedException {
				
		
		String[] movie_rating_pair = value.toString().split("~");
               
                for(int i = 0 ; i < movie_rating_pair.length; i++){   
                   String[] movRateA= movie_rating_pair[i].split("%%");
                    String movieA = movRateA[0]; 
                    String rateA = movRateA[1];
			for(int j= i+1 ; j < movie_rating_pair.length; j++){		
			
			String[] movRateB = movie_rating_pair[j].split("%%");
				
				String movieB = movRateB[0];
				String rateB = movRateB[1];
                                
                                int compare = movieA.compareTo(movieB);
                                if(compare < 0)
                                {
                                    context.write(new Text(movieA+"~"+movieB), new Text(rateA+","+rateB));
                                }
                                else
                                {
                                   context.write(new Text(movieB+"~"+movieA), new Text(rateB+","+rateA)); 
                                }                     
                     }
        }
}
}
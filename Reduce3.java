
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author swarna
 */
public class Reduce3 extends Reducer<Text,Text,Text,Text>
{
protected void reduce(Text key,Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context)
			throws IOException, InterruptedException {
                Double rateMul = new Double(0);
		Double sqrtSum1 = new Double(0);
		Double sqrtSum2 = new Double(0);
		Double Cosine_similarity = new Double(0);
                Double Correlation_coefficient = new Double(0);
                Double Total_similarity = new Double(0);
                int SimilarRatingPair_Count=0;
		
               ArrayList<Double> movieslistA=new ArrayList<Double>();
                       
             ArrayList<Double> movieslistB=new ArrayList<Double>();
             
                for(Text val:values)
                {
                    String[] Rate_Pair = val.toString().split(",");
			Double rateA = Double.valueOf(Rate_Pair[0]);
			Double rateB = Double.valueOf(Rate_Pair[1]);
			
			rateMul += rateA * rateB;
			sqrtSum1 += rateA * rateA;
			sqrtSum2 += rateB * rateB;
                        movieslistA.add(rateA);
                        movieslistB.add(rateB);
                        
		}
                if(sqrtSum1 == 0 || sqrtSum2 == 0){
			context.write(key, new Text("0"));
                }
                
                else{
			Cosine_similarity = rateMul / (Math.sqrt(sqrtSum1) * Math.sqrt(sqrtSum2));//Cosine Similarity 
                        Correlation_coefficient=Recommender.CorrelationCoefficient(movieslistA,movieslistB);
                        SimilarRatingPair_Count=Recommender.RatingPair_count(movieslistA,movieslistB);
			Total_similarity=(0.5*Cosine_similarity)+(0.5*Correlation_coefficient);
                        context.write(key, new Text("%"+Total_similarity.toString()+"%"+Cosine_similarity.toString()+"%"+Correlation_coefficient.toString()+"%"+SimilarRatingPair_Count)); 	
		}
	}
}
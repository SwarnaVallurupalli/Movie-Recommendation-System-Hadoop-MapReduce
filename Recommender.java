
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author swarna
 */
public class Recommender {
    
    public static void TopKMovies(String address) throws IOException{
		String Entered_Movie=" ";
                Double Minimum_value=0.0;
                Integer Minimum_RPairs=0;
		Integer k = 0;
     
                System.out.println("Please enter movies name split with flag '-m~'%number of similar movies%similarity lower bound%minimum rating pairs");
		Scanner scanner = new Scanner(System.in);
                 String entered_string= scanner.nextLine();
                 String[] entered_values=entered_string.split("%");
                String[] Entered_Movies=entered_values[0].split("-m~");
                for(int m=0;m<3;m++)
                {
                 Entered_Movie=Entered_Movies[m];
                 k=Integer.valueOf(entered_values[1]);
                 Minimum_value=Double.valueOf(entered_values[2]);
                 Minimum_RPairs=Integer.valueOf(entered_values[3]);  
		Configuration configuration = new Configuration();
		FileSystem fs;
		BufferedReader reader = null;
		BufferedWriter writer = null;
		
			fs = FileSystem.get(configuration);
		
			FSDataInputStream inputStream = fs.open(new Path(address+"/part-r-00000"));	
			reader = new BufferedReader(new InputStreamReader(inputStream));
			HashMap<String,Data> map = new HashMap<>();
			ArrayList<Map.Entry<String,Data>> arrayList;
                        ArrayList<Data> listall=new ArrayList<Data>();
			String aLine = null;
			while ((aLine = reader.readLine()) !=null) {
                                String[] split = aLine.split("%");
				String Key = split[0]; 
                                String[] Movie_Pairs= Key.split("~");
                                String MovieA=Movie_Pairs[0];
                                String MovieB=Movie_Pairs[1]; 
                                if(Entered_Movie.equalsIgnoreCase(MovieA)||Entered_Movie.equalsIgnoreCase(MovieB))
                                {
                                    if(Double.valueOf(split[1])>= Minimum_value && Double.valueOf(split[4])>=Minimum_RPairs)
                                    { 
                                        if(Entered_Movie.equalsIgnoreCase(MovieA))
                                        {
                                           Data data=new Data(Entered_Movie,MovieB,Double.valueOf(split[1]),Double.valueOf(split[2]),Double.valueOf(split[3]),Double.valueOf(split[4]));
                                           listall.add(data);
                                        }
                                        else
                                        {
                                         Data data=new Data(Entered_Movie,MovieA,Double.valueOf(split[1]),Double.valueOf(split[2]),Double.valueOf(split[3]),Double.valueOf(split[4]));
                                         listall.add(data);
                                        }
                                    }
                                }
                        }               
                        	
        Collections.sort(listall,new Comparator<Data>() {
        @Override
        public int compare(Data a, Data b) {
        return b.getTotalSimilarity().compareTo(a.getTotalSimilarity());
    }
});    
                        if(k > listall.size())
                        {
                            k=listall.size();
                        }
                        
			 for(int i=0;i<k;i++)
                        {
                           System.out.println(listall.get(i).toString());
                        }			
}
}

public static Double CorrelationCoefficient(ArrayList<Double> A,ArrayList<Double> B)
{
    int n=A.size();
    double r,nr=0,dr_1=0,dr_2=0,dr_3=0,dr=0;
    
    double xx[]=new double[n];
    double xy[]=new double[n];
    double yy[]=new double[n];
    double x[]=new double[n];
    double y[]=new double[n];
    double sum_y=0,sum_yy=0,sum_xy=0,sum_x=0,sum_xx=0;
   
    for (int i=0;i<n;i++) {
			x[i]=A.get(i);
                        y[i]=B.get(i);
                        xx[i]=x[i]*x[i];
                        yy[i]=y[i]*y[i];
		}
     for(int i=0;i<n;i++)
    {
    sum_x+=x[i];
    sum_y+=y[i];
    sum_xx+= xx[i];
    sum_yy+=yy[i];
    sum_xy+= x[i]*y[i];
    }
     nr=(n*sum_xy)-(sum_x*sum_y);
    double sum_x2=sum_x*sum_x;
    double sum_y2=sum_y*sum_y;
    dr_1=(n*sum_xx)-sum_x2;
    dr_2=(n*sum_yy)-sum_y2;
    dr_3=dr_1*dr_2;
    dr=Math.sqrt(dr_3);
    r=(nr/dr);
    
    if(dr==0)
    {
        r=0;
    }
    return r;
}

public static int RatingPair_count(ArrayList<Double> A,ArrayList<Double> B)
{
    int counter=0;
for(int i=0;i<A.size();i++)
{
   
    int compare = A.get(i).compareTo(B.get(i));
    
    if(compare==0)
    {
        counter+=1;
    }
} 
    return counter;
}

    
}

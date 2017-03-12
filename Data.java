

/**
 *
 * @author swarna
 */
public class Data
        
{
    private String EnteredMovieName=" ";
    private String MovieName="";
    private Double TotalSimilarity=0.0;
    private Double CorrCoef=0.0;
    private Double CosineSimilarity=0.0;
    private Double Similar_RPair=0.0;

        public Data(String enteredMovieName,String movieName,Double totalSimilarity,Double corrCoef,Double cosineSimilarity,Double similar_RPair) {
       
          EnteredMovieName=enteredMovieName;  
         MovieName=movieName;
            TotalSimilarity=totalSimilarity;
            CorrCoef=corrCoef;
            CosineSimilarity=cosineSimilarity;
            Similar_RPair=similar_RPair;
        
        }

        public String getEnteredMovieName() {
            return EnteredMovieName;
        }

        public void setEnteredMovieName(String EnteredMovieName) {
            this.EnteredMovieName = EnteredMovieName;
        }

        public String getMovieName() {
            return MovieName;
        }

        public void setMovieName(String MovieName) {
            this.MovieName = MovieName;
        }

        public Double getTotalSimilarity() {
            return TotalSimilarity;
        }

        public void setTotalSimilarity(Double TotalSimilarity) {
            this.TotalSimilarity = TotalSimilarity;
        }

        public Double getCorrCoef() {
            return CorrCoef;
        }

        public void setCorrCoef(Double CorrCoef) {
            this.CorrCoef = CorrCoef;
        }

        public Double getCosineSimilarity() {
            return CosineSimilarity;
        }

        public void setCosineSimilarity(Double CosineSimilarity) {
            this.CosineSimilarity = CosineSimilarity;
        }

        public Double getSimilar_RPair() {
            return Similar_RPair;
        }

        public void setSimilar_RPair(Double Similar_RPair) {
            this.Similar_RPair = Similar_RPair;
        }

        @Override
        public String toString() {
            return "Data{" + "EnteredMovieName=" + EnteredMovieName + ", MovieName=" + MovieName + ", TotalSimilarity=" + TotalSimilarity + ", CorrCoef=" + CorrCoef + ", CosineSimilarity=" + CosineSimilarity + ", Similar_RPair=" + Similar_RPair + '}';
        }
}
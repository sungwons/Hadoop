
package movieratings;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MovieRatingsReducer extends Reducer<Text, Text, Text, Text> {
    
    Logger logger = Logger.getLogger(MovieRatingsMapper.class);
    
    int i = 0;
    int j = 0;    
    Text val = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
        i = 0;
        j = 0;
        
        for(Text val : values) {   
            String line = val.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,"\t");
            
            if(tokenizer.countTokens() == 3) {  
                // u.item
                String title = tokenizer.nextToken();            
                String release = tokenizer.nextToken();
                String imdb = tokenizer.nextToken();
                val.set(title + "\t" + release + "\t" + imdb);
            }
            else { 
                // u.data
                int rating = Integer.parseInt(tokenizer.nextToken());
                j += rating;
                i = i + 1;
            }
            
        }
        
        double avg = (double)j/(double)i;
        val.set(val.toString() + "\t" + String.format("%.2f", avg) + "\t" + Integer.toString(i) + "\t" + Integer.toString(j));
        
        context.write (key, val);
        
        //UniqueMovies counter
        Counter counter = context.getCounter(MyCounter.UNIQUE_MOVIES);
        counter.increment(1);
        
        logger.debug("DEBUG - PROCESS A RECORD");
    }       
}
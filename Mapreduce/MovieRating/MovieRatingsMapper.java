
package movieratings;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

public class MovieRatingsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    Logger logger = Logger.getLogger(MovieRatingsMapper.class);
    Text id = new Text();
    Text detail = new Text();
    
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger.info("in setup of " + context.getTaskAttemptID().toString());
        logger.debug("DEBUG - IN MAPPER SETUP");
        String fileName = ((FileSplit) context.getInputSplit()).getPath() + "";
        System.out.println ("in stdout"+ context.getTaskAttemptID().toString() + " " +  fileName);
        System.err.println ("in stderr"+ context.getTaskAttemptID().toString());
    }    
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, "\t");
        
        if(tokenizer.countTokens() == 4) {
            // u.data 
            tokenizer.nextToken();
            String itemid = tokenizer.nextToken();
            String rating = tokenizer.nextToken();
            id.set(itemid);
            detail.set(rating);
            context.write(id, detail);
        }
        else {
            // u.item
            tokenizer = new StringTokenizer(line, "|");            
            String itemid = tokenizer.nextToken();   // 1st: movieid
            String title = tokenizer.nextToken();    // 2nd: movie tile
            String release = tokenizer.nextToken();  // 3rd: release date
            String imdb = tokenizer.nextToken();     // 4th: IMDB URL
            detail.set(title + "\t" + release + "\t" + imdb);
            id.set(itemid);
            context.write(id, detail);
        }

        //TotalRecords counter
        Counter counter = context.getCounter(MyCounter.TOTAL_RECORDS);
        counter.increment(1);
        
        logger.debug("DEBUG - PROCESS A RECORD");
    } 
    
    @Override
    protected void cleanup(Context context){
        logger.debug("DEBUG - IN MAPPER CLEANUP");
    }
    
    
}

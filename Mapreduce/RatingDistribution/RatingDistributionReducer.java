
package ratingdistribution;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

class RatingDistributionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    Logger logger = Logger.getLogger(RatingDistributionMapper.class);
    int i = 0;
    IntWritable count = new IntWritable();
    
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        
        i = 0;

        for (IntWritable val : values){
            i = i + 1;                                  
        }
        
       count.set(i);
       context.write (key, count);
       
       logger.debug("DEBUG - PROCESS A RECORD");
       
    }
    
    protected void cleanup(Mapper.Context context){
       logger.debug("DEBUG - IN REDUCER CLEANUP");
    }
    
}

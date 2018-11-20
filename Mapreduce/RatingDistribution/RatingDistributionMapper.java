
package ratingdistribution;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;


class RatingDistributionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    Logger logger = Logger.getLogger(RatingDistributionMapper.class);
    IntWritable one = new IntWritable();
    Text txt = new Text();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger.info("in setup of " + context.getTaskAttemptID().toString());
        String fileName = ((FileSplit) context.getInputSplit()).getPath() + "";
        System.out.println ("in stdout"+ context.getTaskAttemptID().toString() + " " +  fileName);
        System.err.println ("in stderr"+ context.getTaskAttemptID().toString());
        logger.debug("DEBUG - IN MAPPER SETUP");
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
        String line = value.toString();
//        line = line.replaceAll("],.;?\"!\\/]", "");
        StringTokenizer tokenizer = new StringTokenizer(line, "\t");      
        
//        String userId = tokenizer.nextToken();
        tokenizer.nextToken();
        tokenizer.nextToken();
        String rating = tokenizer.nextToken();
//        String timestamp = tokenizer.nextToken();
        txt.set(tokenizer.nextToken());       
        context.write(txt, one);
        
        logger.debug("DEBUG - PROCESS A RECORD");
    
    }
    
    @Override
    protected void cleanup(Context context){
         logger.debug("DEBUG - IN MAPPER CLEANUP");
    }
    
}

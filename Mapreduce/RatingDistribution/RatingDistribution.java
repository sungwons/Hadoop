
package ratingdistribution;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class RatingDistribution {
 
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
//        String inputPath = args[0];
//        String outputPath = args[1];
        
        String inputPath = "/movie-ratings";
        String outputPath = "/movie-rating-distribution";
        
        Job ratingJob = null;
        
        Configuration conf = new Configuration();
        
        conf.set("mapreduce.map.log.level", "DEBUG");
        conf.set("mapreduce.reduce.log.level", "DEBUG");
        
        // Set the Number of Reducers
        conf.setInt("mapreduce.job.reduces", 5);
        
        ratingJob = Job.getInstance(conf, "RatingDistribution");
        
        // Input Path
        FileInputFormat.addInputPath(ratingJob, new Path(inputPath));
        
        // Input Data Format
        ratingJob.setInputFormatClass(TextInputFormat.class);
        
        // Mapper and Reducer Class
        ratingJob.setMapperClass(RatingDistributionMapper.class);
        ratingJob.setReducerClass(RatingDistributionReducer.class);
        
        // Set the Jar file
        ratingJob.setJarByClass(ratingdistribution.RatingDistribution.class);
        
        // Output Path
        FileOutputFormat.setOutputPath(ratingJob, new Path(outputPath));
        
        // Ouput Data Format
        ratingJob.setOutputFormatClass(TextOutputFormat.class);
        
        // Set the Output Key and Value Class
        ratingJob.setOutputKeyClass(Text.class);
        ratingJob.setOutputValueClass(IntWritable.class);
        
//        // Set the Number of Reducers
//        ratingJob.setNumReduceTasks(3);
              
        try {
            ratingJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
}

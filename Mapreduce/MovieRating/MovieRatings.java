
package movieratings;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MovieRatings {

    
    public static void main(String[] args) throws IOException {
        
        Job movieRatingJob = null;
        
//        String inputPath = args[0];
//        String outputPath = args[1];
        
        String inputPath = "/movie-and-ratings";
        String outputPath = "/movie-rating-result";
          
        Configuration conf = new Configuration();
        
        conf.set("mapreduce.map.log.level", "DEBUG");
        conf.set("mapreduce.reduce.log.level", "DEBUG");
        
        // Job Input Compression
        conf.setStrings("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec");
        
        // Job Output Compression
        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
        conf.setStrings("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
        
        // Intermediate Mapper/Reducer Compression
        conf.setBoolean("mapreduce.output.compress", true);
        conf.setStrings("mapreduce.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
              
        // Set the Number of Reducers
        conf.setInt("mapreduce.job.reduces", 2);
        
        movieRatingJob = Job.getInstance(conf, "MovieRatings");
        
        // Input Path
        FileInputFormat.addInputPath(movieRatingJob, new Path(inputPath));
        
        // Input Data Format
        movieRatingJob.setInputFormatClass(TextInputFormat.class);
        
        // Mapper and Reducer Class
        movieRatingJob.setMapperClass(MovieRatingsMapper.class);
//        movieRatingJob.setCombinerClass(MovieRatingsCombiner.class);
        movieRatingJob.setReducerClass(MovieRatingsReducer.class);
        
        // Set the Jar file
        movieRatingJob.setJarByClass(movieratings.MovieRatings.class);
        
        // Output Path
        FileOutputFormat.setOutputPath(movieRatingJob, new Path(outputPath));
        
        // Ouput Data Format
        movieRatingJob.setOutputFormatClass(TextOutputFormat.class);
        
        // Set the Output Key and Value Class
        movieRatingJob.setOutputKeyClass(Text.class);
        movieRatingJob.setOutputValueClass(Text.class);        
        
        // Set the Number of Reducers
//        movieRatingJob.setNumReduceTasks(2);
              
        try {
            movieRatingJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }  
    
}

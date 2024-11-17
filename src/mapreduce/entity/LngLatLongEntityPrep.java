// LngLatLongEntityPrep.java
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LngLatLongEntityPrep {
    public static class LngLatLongMapper 
            extends Mapper<Object, Text, Text, NullWritable> {
        
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            if (value.toString().startsWith("language_code")) {
                return;
            }
            
            String[] fields = value.toString().split("\t");
            if (fields.length > 6) {  // Make sure we have latitude and longitude fields
                String langCode = fields[0].trim();
                String latitude = fields[5].trim();  // approximate_latitude
                String longitude = fields[6].trim(); // approximate_longitude
                
                if (!langCode.isEmpty() && !latitude.isEmpty() && !longitude.isEmpty()) {
                    // Format output as: langCode\tlatitude\tlongitude
                    context.write(new Text(langCode + "\t" + latitude + "\t" + longitude), 
                                NullWritable.get());
                }
            }
        }
    }

    public static class LngLatLongReducer 
            extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "language latlong entity prep");
        job.setJarByClass(LngLatLongEntityPrep.class);
        job.setMapperClass(LngLatLongMapper.class);
        job.setReducerClass(LngLatLongReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
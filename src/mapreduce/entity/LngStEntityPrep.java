// LngStEntityPrep.java
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

public class LngStEntityPrep {
    public static class LngStMapper 
            extends Mapper<Object, Text, Text, NullWritable> {
        
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            if (value.toString().startsWith("language_code")) {
                return;
            }
            
            String[] fields = value.toString().split("\t");
            if (fields.length > 7) {  // state_territory is eighth field
                String[] states = fields[7].split(",");  // Split on comma
                for (String state : states) {
                    String trimmedState = state.trim();
                    if (!trimmedState.isEmpty()) {
                        context.write(new Text(trimmedState), NullWritable.get());
                    }
                }
            }
        }
    }

    public static class LngStReducer 
            extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "language st entity prep");
        job.setJarByClass(LngStEntityPrep.class);
        job.setMapperClass(LngStMapper.class);
        job.setReducerClass(LngStReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
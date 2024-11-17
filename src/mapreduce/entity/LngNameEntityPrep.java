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

public class LngNameEntityPrep {
    public static class LngNameMapper 
            extends Mapper<Object, Text, Text, NullWritable> {
        
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            if (value.toString().startsWith("language_code")) {
                return;
            }
            
            String[] fields = value.toString().split("\t");
            if (fields.length > 1) {  // language_name is second field
                String[] names = fields[1].split("/");  // Split on forward slash
                for (String name : names) {
                    String trimmedName = name.trim();
                    if (!trimmedName.isEmpty()) {
                        context.write(new Text(trimmedName), NullWritable.get());
                    }
                }
            }
        }
    }

    public static class LngNameReducer 
            extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "language name entity prep");
        job.setJarByClass(LngNameEntityPrep.class);
        job.setMapperClass(LngNameMapper.class);
        job.setReducerClass(LngNameReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

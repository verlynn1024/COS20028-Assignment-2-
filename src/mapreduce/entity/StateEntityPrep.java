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

public class StateEntityPrep {
    public static class StateMapper 
            extends Mapper<Object, Text, Text, NullWritable> {
        
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length >= 8) {
                String stateField = fields[7].trim();  // state_territory field
                if (!stateField.isEmpty()) {
                    // Handle multiple states that might be separated by commas
                    String[] states = stateField.split(",");
                    for (String state : states) {
                        context.write(new Text(state.trim()), NullWritable.get());
                    }
                }
            }
        }
    }

    public static class StateReducer 
            extends Reducer<Text, NullWritable, Text, NullWritable> {
        
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            // Emit each unique state once
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "state entity prep");
        job.setJarByClass(StateEntityPrep.class);
        job.setMapperClass(StateMapper.class);
        job.setReducerClass(StateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
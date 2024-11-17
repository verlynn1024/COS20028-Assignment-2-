// LngSynonymEntityPrep.java
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

public class LngSynonymEntityPrep {
    public static class LngSynonymMapper 
            extends Mapper<Object, Text, Text, NullWritable> {
        
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            if (value.toString().startsWith("language_code")) {
                return;
            }
            
            String[] fields = value.toString().split("\t");
            if (fields.length > 2) {  // language_synonym is third field
                String[] synonyms = fields[2].split(",");  // Split on comma
                for (String synonym : synonyms) {
                    String trimmedSynonym = synonym.trim();
                    if (!trimmedSynonym.isEmpty()) {
                        context.write(new Text(trimmedSynonym), NullWritable.get());
                    }
                }
            }
        }
    }

    public static class LngSynonymReducer 
            extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "language synonym entity prep");
        job.setJarByClass(LngSynonymEntityPrep.class);
        job.setMapperClass(LngSynonymMapper.class);
        job.setReducerClass(LngSynonymReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
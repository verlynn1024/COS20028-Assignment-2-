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



public class LngCodeEntityPrep {

    public static class LngCodeMapper 

            extends Mapper<Object, Text, Text, NullWritable> {

        

        public void map(Object key, Text value, Context context) 

                throws IOException, InterruptedException {

            // Skip header row

            if (value.toString().startsWith("language_code")) {

                return;

            }

            

            String[] fields = value.toString().split("\t");

            if (fields.length > 0) {

                String langCode = fields[0].trim();

                if (!langCode.isEmpty()) {

                    context.write(new Text(langCode), NullWritable.get());

                }

            }

        }

    }



    public static class LngCodeReducer 

            extends Reducer<Text, NullWritable, Text, NullWritable> {

        

        public void reduce(Text key, Iterable<NullWritable> values, Context context)

                throws IOException, InterruptedException {

            // Output unique codes

            context.write(key, NullWritable.get());

        }

    }



    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "language code entity prep");

        job.setJarByClass(LngCodeEntityPrep.class);

        job.setMapperClass(LngCodeMapper.class);

        job.setReducerClass(LngCodeReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}

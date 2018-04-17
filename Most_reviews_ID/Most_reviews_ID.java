package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;
import java.util.TreeMap;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class Most_reviews_ID extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new Most_reviews_ID(), args);

        System.exit(res);
    }
    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Most_reviews_ID.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;


}
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

                JsonParser parser = new JsonParser();
                JsonElement element = parser.parse(value.toString());
                String reviews_ID = element.getAsJsonObject().get("reviewerID").getAsString();

                context.write(new Text(reviews_ID), ONE);
            }
        }
 public static class Reduce extends Reducer<Text , IntWritable, Text, IntWritable> {

        int min = 9999999;
        Text ID = new Text();
	IntWritable num = new IntWritable(0);
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if(sum < min)
            {
		min = sum;
		ID.set(key);
           	num = new IntWritable(min); 
	   }
        }
	@Override
        public void cleanup(Context context) throws IOException, InterruptedException{ 
        context.write(ID, num);
    }


	}
  }

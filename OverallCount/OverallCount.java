package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
// add
import org.apache.hadoop.io.FloatWritable;
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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class OverallCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new OverallCount(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(OverallCount.class);
	job.setOutputKeyClass(FloatWritable.class); 
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

    public static class Map extends Mapper<LongWritable, Text, FloatWritable, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);

 	@Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
                
		JsonParser parser = new JsonParser();
		JsonElement element = parser.parse(value.toString());	
		float overall = element.getAsJsonObject().get("overall").getAsFloat();
		
		context.write(new FloatWritable(overall), ONE);
            }
        }

    public static class Reduce extends Reducer<FloatWritable , IntWritable, FloatWritable, IntWritable> {

	float total_sum = 0;
        int cnt = 0;
	@Override
        public void reduce(FloatWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
            total_sum += (key.get()*sum);
	    cnt += sum;

	    if(key.get() >= 5.0 )
	    {
		float avg = total_sum/cnt;
		context.write(new FloatWritable(avg), new IntWritable(cnt));
	    }
        }
    }
}


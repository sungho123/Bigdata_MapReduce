package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

public class Max_A_reviews_ID extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new Max_A_reviews_ID(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Max_A_reviews_ID.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }

 public static class Map extends Mapper<LongWritable, Text, Text,Text> {
        Gson gson = new Gson();
        int[] data;
        String ID =" ";
        String tmp = "";
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

                JsonParser parser = new JsonParser();
                JsonElement element = parser.parse(value.toString());
                ID = element.getAsJsonObject().get("reviewerID").getAsString();

                data = gson.fromJson(element.getAsJsonObject().get("helpful"), int[].class);

                tmp = Integer.toString(data[0])+","+Integer.toString(data[1]);
                context.write(new Text(ID),new Text(tmp));
            }
        }

 public static class Reduce extends Reducer<Text, Text, Text, Text> {

	private HashMap<String, String> tmpMap = new HashMap<String, String>();
        int max_A = 0;
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
            int sum_A = 0;
            for(Text val : values){
                sum_A += Integer.parseInt(val.toString().split(",")[0]);
            }
	   if(sum_A > max_A)
	   {
		max_A = sum_A;
		tmpMap.put(key.toString(), Integer.toString(max_A));
	  }
        }
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
                for(Entry<String, String> entry : tmpMap.entrySet())
                {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        if(max_A == Integer.parseInt(value)){
                                context.write(new Text(key), new Text(value));
                        }
                }
	}
}
}

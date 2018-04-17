package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;


import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class Helpful_asin_test extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new Helpful_asin_test(), args);

        System.exit(res);
    }
    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Helpful_asin_test.class);
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

 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        Gson gson = new Gson();
	float[] data;
	String asin =" ";
	String tmp = " ";	
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

                JsonParser parser = new JsonParser();
                JsonElement element = parser.parse(value.toString());
                asin = element.getAsJsonObject().get("asin").getAsString();
		data = gson.fromJson(element.getAsJsonObject().get("helpful"), float[].class);

		tmp = Float.toString(data[0])+","+Float.toString(data[1]);
                context.write(new Text(asin),new Text(tmp));
		
	    }	
	} 

public static class Reduce extends Reducer<Text , Text, Text, Text> {

	private HashMap<String,String> tmpMap = new HashMap<String,String>();
        float positive = 0;
        float total =0;
	float max = 0;
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
          float sum_A = 0;
	  float sum_B = 0; 
	  for (Text val : values) {
                sum_A += Float.parseFloat(val.toString().split(",")[0]);
                sum_B += Float.parseFloat(val.toString().split(",")[1]);
                }
		if(sum_A != 0 && sum_B >= 10)
		{
			float total_rate = sum_A/sum_B;
			
			if(total_rate > max)
			{
				max = total_rate;
				tmpMap.put(key.toString(), Float.toString(total_rate));
			}
		}
     }
	@Override
        public void cleanup(Context context) throws IOException, InterruptedException{
                for(Entry<String, String> entry : tmpMap.entrySet())
                {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        if(max == Float.parseFloat(value)){
                                context.write(new Text(key), new Text(value));
                        }
                }
        }
}
}


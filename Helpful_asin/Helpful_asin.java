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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
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

public class Helpful_asin extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new Helpful_asin(), args);

        System.exit(res);
    }
    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Helpful_asin.class);
   	job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;


}

 public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        Gson gson = new Gson();
	double[] data;
	String asin =" ";	
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

                JsonParser parser = new JsonParser();
                JsonElement element = parser.parse(value.toString());
                asin = element.getAsJsonObject().get("asin").getAsString();
		data = gson.fromJson(element.getAsJsonObject().get("helpful"), double[].class);
		
		if(data[0] != 0 && data[1] !=0 && data[1] >=10){
		        double tmp = data[0]/data[1];
                        context.write(new Text(asin), new DoubleWritable(tmp));	
		}
   }	
}	 

public static class Reduce extends Reducer<Text , DoubleWritable, Text, DoubleWritable> {

	private HashMap<String,Double> tmpMap = new HashMap<String,Double>();
        double max = 0;
	Text asin = new Text();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
          double sum = 0.0;
	  double total = 0.0;  
	  for (DoubleWritable val : values) {
        	sum  = val.get();  
	
		if(sum >= max)
 		{
			max = sum;
         	        asin.set(key);
			tmpMap.put(key.toString(), max);
		}
     	   }	
	}
	@Override
        public void cleanup(Context context) throws IOException, InterruptedException{ 
		for(Entry<String, Double> entry : tmpMap.entrySet())
		{
			String key = entry.getKey();
			double value = entry.getValue();
			if(max == value){
				context.write(new Text(key), new DoubleWritable(value));
			}
    		}
	}

  } 
}

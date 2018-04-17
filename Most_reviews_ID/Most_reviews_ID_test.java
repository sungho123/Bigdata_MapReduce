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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;
import java.util.TreeMap;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;


 public class Most_reviews_ID_test {


  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static LongWritable ONE = new LongWritable(1);
    String date = " ";
 	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
	 JsonParser parser = new JsonParser();
         JsonElement element = parser.parse(value.toString());
         date = element.getAsJsonObject().get("reviewTime").getAsString();
	 String tmp  = date.split(",")[1]; 	 

         context.write(new Text(tmp), ONE);
  }
}

 public static class KeyValueSwappingMapper extends Mapper<Text, LongWritable, LongWritable, Text> {
	int cnt = 0;
    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
	context.write(value, key);	
  }
}

  public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
        InterruptedException {
    	long sum = 0;
	for(LongWritable val : values)
	{
		sum += val.get();
	}
	result.set(sum);
	context.write(key, result);
	}
}

/*   public static class KeyvalueReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
   private LongWritable result = new LongWritable();
   int max = 0;
    public void reduce(Text key, LongWritable values, Context context) throws IOException,
        InterruptedException {
		for(Text ID : key)
		{
                context.write(ID, values);
		max ++;
		if(max == 20)
		{
			break;
		}
		}
		}
	
}


 */ 
        public static void main(String[] args) throws Exception {

        System.out.println(Arrays.toString(args));
        Configuration conf = new Configuration();
        Path out = new Path(args[1]);

        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(Most_reviews_ID_test.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(SumReducer.class);
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(out,"out1"));
        if (!job1.waitForCompletion(true)) {
        System.exit(1);
        }

	Job job2 = Job.getInstance(conf);
        job2.setJarByClass(Most_reviews_ID_test.class);
        job2.setMapperClass(KeyValueSwappingMapper.class);
	job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
	job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
	FileInputFormat.addInputPath(job2, new Path(out,"out1"));
        FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
        if (!job2.waitForCompletion(true)) {
          System.exit(1);
        }
}
}

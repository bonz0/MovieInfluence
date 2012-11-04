import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import redis.clients.jedis.Jedis;

public class InLinks {

	public static final String inLinks = "inLinks";
	
	static Jedis jedisServer = new Jedis("localhost");

	public static class InLinksMapper extends Mapper<Object, Text, Text, Text> {
		protected void map (Object Key, Text rowText, Context context) throws IOException, InterruptedException {
			String[] columns = rowText.toString().split(";");
			context.write(new Text(columns[0]), new Text(columns[1]));
		}
	}	

public static class InLinksReducer extends Reducer<Text, Text, Text, Text> {
	     public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				for(Text text: values) {
					jedisServer.rpush(inLinks.concat(key.toString()), text.toString());
				}
				int count = 0;
				context.write(key, new Text(count + ""));
			}
	 }

	public static void main(String[] args) throws Exception {
		Job job = new Job(new Configuration(), "InLinks");
		job.setMapperClass(InLinksMapper.class);
		job.setReducerClass(InLinksReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

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
import java.util.List;

import redis.clients.jedis.Jedis;

public class PageRank {

	public static final String pageRank = "pageRank";
	public static final String inLinks = "inLinks";
	public static final String outLinkCount = "outLinkCount";

	public static final double d = 0.85;
	public static final double epsilon = 0.00000009;
	public static final String hasConverged = "hasConverged";

	static Jedis jedisServer = new Jedis("localhost");

	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
		protected void map (Object Key, Text rowText, Context context) throws IOException, InterruptedException {
			String[] columns = rowText.toString().split("\t", 2);
			double currentPageRank = calculatePageRank(columns[0]);
			context.write(new Text(columns[0]), new Text(columns[1] + "\t" + Double.toString(currentPageRank)));
		}

		private static double calculatePageRank(String key) {
			double defaultPageRank = (1 - d);
			jedisServer.setnx(pageRank.concat(key), Double.toString(defaultPageRank));
			List<String> inLinksList = jedisServer.lrange(inLinks.concat(key), 0, -1);
			if(inLinksList.size() > 0) {
				double linkRanks = 0.0;
				for(String singleInLink: inLinksList) {
					jedisServer.setnx(pageRank.concat(singleInLink), Double.toString(defaultPageRank));
					linkRanks += Double.parseDouble(jedisServer.get(pageRank.concat(singleInLink))) / Double.parseDouble(jedisServer.get(outLinkCount.concat(singleInLink)));
				}
				defaultPageRank += linkRanks * d;
			}
			double previousPageRank = Double.parseDouble(jedisServer.get(pageRank.concat(key)));
			jedisServer.set(pageRank.concat(key), Double.toString(defaultPageRank));
			if(Math.abs(previousPageRank - defaultPageRank) > epsilon) {
				jedisServer.set(hasConverged, "false");
			}
			return defaultPageRank;
		}
	}	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path(args[1]);
		boolean converged = false;
		jedisServer.setnx(hasConverged, "true");
		int iterationCount = 1;
		while(!converged) {
			if(fileSystem.exists(path)) {
				System.out.println("-----------------------------Directory Exists-----------------------------");
				fileSystem.delete(path, true);
				System.out.println("-----------------------------Directory Deleted-----------------------------");
			}
			jedisServer.set(hasConverged, "true");
			System.out.println("Iteration number: " + iterationCount++);
			Job job =  new Job(conf, "PageRank");
			job.setMapperClass(PageRankMapper.class);
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true); 
			String convergedString = jedisServer.get(hasConverged);
			converged = "true".equals(convergedString) ? true : false;
		}
		fileSystem.close();
	}
}

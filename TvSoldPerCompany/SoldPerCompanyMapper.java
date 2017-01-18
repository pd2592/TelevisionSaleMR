package TvSoldPerCompany;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class SoldPerCompanyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	Text outkey = new Text();
	IntWritable outvalue = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
				
		String company = value.toString().split("\\|")[0];
		outkey.set(company);
		context.write(outkey, outvalue);
		
	}
}

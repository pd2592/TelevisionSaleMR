package OnidaSale;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class OnidaStateWiseMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	Text outkey = new Text();
	IntWritable outvalue = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String state;		
		String company = value.toString().split("\\|")[0];
		if(company.equalsIgnoreCase("Onida")){
			
			state = value.toString().split("\\|")[3];
			outkey.set(state);
			context.write(outkey, outvalue);
			
			
		}
	}
}

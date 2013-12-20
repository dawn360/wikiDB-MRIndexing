import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * To define a reduce function for your MapReduce job, subclass
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 * @param The data type of the input key - Text
 * @param The data type of the input value - IntWritable
 * @param The data type of the output key - Text
 * @param The data type of the output value - DoubleWritable
 */
public class GDIndexReducer extends
    Reducer<Text, IntWritable, Text, Text> {

  /**
   * The reduce method runs once for each key received from
   * the shuffle and sort phase of the MapReduce framework.
   * The method receives:
   * @param A key of type Text
   * @param A set of values of type IntWritable
   * @param A Context object
   */
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    String line="";
	int cnt=1; //counter
	//get first value for comparison
	int first= values.iterator().next().get();
	
    for(IntWritable value :values){
      //if first docID is repeating count++
    	if(first == value.get())
		cnt+=1;
	else
	{
		//if new docID dump first,count and starting counting with
		//new docID
		line+="("+first+","+cnt+")";
		cnt=1;
		first=value.get();
	}
    }
	//dump last count
	line+="("+first+","+cnt+")";
      
	context.write(key, new Text(line));
	
  }
}

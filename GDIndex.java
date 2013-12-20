/*
 * TO DO implement
 * Provide additional import statements here
 */
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GDIndex extends Configured implements Tool {

  public int run(String[] args) throws Exception {

    if (args.length != 3) {
      System.out.printf("Usage: GDIndex <input dir> <output dir> <min chars per word to index>\n");
      return -1;
    }
	Configuration conf = getConf();
	conf.set("min_chars_per_word", args[2]);
	conf.set("START_TAG_KEY","<page>");
	conf.set("END_TAG_KEY","</page>");
conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

    Job job = new Job(conf);
    job.setJarByClass(GDIndex.class);
    job.setJobName("Dean & Ghemawat Index");

    /*
     * We are using a KeyValueText file as the input file.
     * Therefore, we must call setInputFormatClass.
     * There is no need to call setOutputFormatClass, because the
     * application uses a text file for output.
     */
    job.setInputFormatClass(XmlInputFormat.class);

    /*
     * Set the input and output paths.
     */
     FileInputFormat.setInputPaths(job,new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    /*
     * Set the mapper and reducer classes.
     */
    job.setMapperClass(GDIndexMapper.class);
    job.setReducerClass(GDIndexReducer.class);


    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    /*
     * Set the output key and value classes.
     */
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new GDIndex(), args);
    System.exit(exitCode);
  }
}

// cc XmlInputFormat An InputFormat for reading a XML file as a record
import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

//vv XmlInputFormat
public class XmlInputFormat
    extends FileInputFormat<LongWritable, Text> {
  
  //@Override
  //protected boolean isSplitable(JobContext context, Path file) {
  //  return false;
 // }

  @Override
 // public RecordReader<NullWritable, BytesWritable> createRecordReader(
  public RecordReader<LongWritable, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    XmlRecordReader reader = new XmlRecordReader();
    reader.initialize(split, context);
    return reader;
  }
}
//^^ XmlInputFormat

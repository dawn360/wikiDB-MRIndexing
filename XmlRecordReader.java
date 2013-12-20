// cc WholeFileRecordReader The RecordReader used by WholeFileInputFormat for reading a whole file as a record
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//vv XmlRecordReader
class XmlRecordReader extends RecordReader<LongWritable, Text> {
  
  private FileSplit fileSplit;
  private Configuration conf;
  private byte[] startTag;
  private byte[] endTag;
  private long start;
  private long end;
  private FSDataInputStream fsin;
  private DataOutputBuffer buffer = new DataOutputBuffer();
  private LongWritable key = new LongWritable();
  private Text value = new Text();

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    this.fileSplit = (FileSplit) split;
    this.conf = context.getConfiguration();
	xmlinit();
  }

private void xmlinit()
{
	try{
	startTag = conf.get("START_TAG_KEY").getBytes("UTF-8");
      endTag = conf.get("END_TAG_KEY").getBytes("utf-8");
     
      // open the file and seek to the start of the split
      start = fileSplit.getStart();
      end = start + fileSplit.getLength();
      Path file = fileSplit.getPath();
      FileSystem fs = file.getFileSystem(conf);
      
	fsin = fs.open(file);
      fsin.seek(start);//cfunction call 
	}catch(Exception e)
	{e.printStackTrace();}
}
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (fsin.getPos() < end) {
        if (readUntilMatch(startTag, false)) {
          try {
            buffer.write(startTag);
            if (readUntilMatch(endTag, true)) {
              key.set(fsin.getPos());
              value.set(buffer.getData(), 0, buffer.getLength());
              return true;
            }
          } finally {
            buffer.reset();
          }
        }
      }
      return false;
  }
  
  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    //return NullWritable.get();
	return key;
  }

  @Override
  public Text getCurrentValue() throws IOException,
      InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException {
    //return processed ? 1.0f : 0.0f;
	return (fsin.getPos() - start) / (float) (end - start);
  }

  @Override
  public void close() throws IOException {
    fsin.close();
  }

private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
      int i = 0;
      while (true) {
        int b = fsin.read();
        // end of file:
        if (b == -1) return false;
        // save to buffer:
        if (withinBlock) buffer.write(b);
        
        // check if we're matching:
        if (b == match[i]) {
          i++;
          if (i >= match.length) return true;
        } else i = 0;
        // see if we've passed the stop point:
        if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
      }
    }
  }


//^^ XmlRecordReader

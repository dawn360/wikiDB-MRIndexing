import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.util.StringTokenizer;
import javax.xml.stream.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
/*
import net.java.textilej.parser.MarkupParser;
import net.java.textilej.parser.builder.HtmlDocumentBuilder;
import net.java.textilej.parser.markup.mediawiki.MediaWikiDialect;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;
import java.io.StringReader;
import java.io.StringWriter;


import java.io.StringWriter;
import java.util.Set;
import java.util.TreeSet;*/


/**
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 * @param The data type of the input key - LongWritable
 * @param The data type of the input value - Text
 * @param The data type of the output key - Text
 * @param The data type of the output value - IntWritable
*/
public class GDIndexMapper extends Mapper<LongWritable, Text, Text,IntWritable> {

  /**
   * The map method runs once for each line of text in the input file.
   * The method receives:
   * @param A key of type LongWritable
   * @param A value of type Text
   * @param A Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	IntWritable docID = new IntWritable();
	//Text token = new Text();

	String contents="";
	boolean isID=true;
	boolean isRedirect=false;
	Configuration conf = context.getConfiguration();
	int min_cpw = Integer.parseInt(conf.get("min_chars_per_word"));


 	XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        InputStream in = new ByteArrayInputStream(value.toString().getBytes("UTF-8"));
	try{
        XMLStreamReader streamReader = inputFactory.createXMLStreamReader(in);
        streamReader.nextTag(); // Advance to "book" element
        streamReader.nextTag();
	//streamReader.nextTag();
	//streamReader.nextTag(); // Advance to "person" element
	//docID.set(streamReader.getElementText());
	String lname="";
        while (streamReader.hasNext()) {
            if (streamReader.isStartElement()) {
                //switch (streamReader.getLocalName()) {
		lname=streamReader.getLocalName();
		if(lname.equalsIgnoreCase("id") && isID){
			docID.set(Integer.parseInt(streamReader.getElementText()));
                    	 isID=false;
                   
                }
		if(lname.equalsIgnoreCase("redirect")){

                    	isRedirect=true;
                }
                if(lname.equalsIgnoreCase("text")){
                    contents=streamReader.getElementText();

                }

            }
		if(isRedirect)
			{break;}
            streamReader.next();
        }
     	} catch (XMLStreamException e) {
            e.printStackTrace();
        }

	if(!isRedirect)
	{
	 // find the the external links section, and cut it away
        int iStartIndex = contents.indexOf("External Links");
	int iEndIndex=0;
        if (iStartIndex < 0)
            iStartIndex = contents.indexOf("External links");
        if (iStartIndex > 0) {
           iEndIndex = contents.indexOf("\n");

            if (iEndIndex < 0)
                contents = contents.substring(0, iStartIndex);
            else {
                String tmp = contents = contents.substring(0, iStartIndex);
                contents = tmp + contents.substring(iEndIndex, contents.length() - 1);
            }
        }
        //Clean up wikitext formating

        // remove all between curly brackets
        contents = contents.replaceAll("\\{\\{[^\\}\\}]*\\}\\}", " ");

        // remove tags like [[en:English]] and [[File:http://foo.bar]]
        contents = contents.replaceAll("\\[\\[[^\\]]+:[^\\]]+\\]\\]", " ");

        // references ...
        contents = contents.replaceAll("\\&lt;ref", "<<<<<");
        contents = contents.replaceAll("/ref\\&gt;", ">>>>>");

        // math ...
        contents = contents.replaceAll("<math>", "<<<<<");
        contents = contents.replaceAll("</math>", ">>>>>");

        // html tags ...
        contents = contents.replaceAll("\\&lt;", "<<<<<");
        contents = contents.replaceAll("\\&gt;", ">>>>>");

        // ... and remove 'em
        contents = contents.replaceAll("<<<<<[^>>>>>]*>>>>>", " ");

        // replace [[word|link]] with link
        contents = contents.replaceAll("\\[\\[[^\\]]+\\|([^\\]]+)\\]\\]", " $1 ");

        // replace [[word#link]] with link
        contents = contents.replaceAll("\\[\\[[^\\]]+#([^\\]]+)\\]\\]", " $1 ");

        // replace [[foo]] with foo
        contents = contents.replaceAll("\\[\\[([^\\]\\]]*)\\]\\]", " $1 ");

        // remove [http://...] references
        contents = contents.replaceAll("\\[http[^\\]]*\\]", " ");

        // some trivia...
        contents = contents.replaceAll("=====", " ");
        contents = contents.replaceAll("====", " ");
        contents = contents.replaceAll("===", " ");
        contents = contents.replaceAll("==", " ");
        contents = contents.replaceAll("'''''", " ");
        contents = contents.replaceAll("''''", " ");
        contents = contents.replaceAll("'''", " ");
        contents = contents.replaceAll("''", " ");
        contents = contents.replaceAll("\\&quot;", " ");
        contents = contents.replaceAll("\\&amp;", " and ");
        contents = contents.replaceAll("\\&ndash;", "-");
        contents = contents.replaceAll("\\&[^\\&]*;", " ");

        contents = contents.replaceAll("[^a-zA-Z]+", " ").toLowerCase();
    

	/*StringWriter writer = new StringWriter();

    HtmlDocumentBuilder builder = new HtmlDocumentBuilder(writer);
    builder.setEmitAsDocument(false);

    MarkupParser parser = new MarkupParser(new MediaWikiDialect());
    parser.setBuilder(builder);
    parser.parse(contents);

    final String html = writer.toString();
    final StringBuilder cleaned = new StringBuilder();

    HTMLEditorKit.ParserCallback callback = new HTMLEditorKit.ParserCallback() {
            public void handleText(char[] data, int pos) {
                cleaned.append(new String(data)).append(' ');
            }
    };
    new ParserDelegator().parse(new StringReader(html), callback, false);
*/
	
	Text token = new Text();


	//String line = value.toString().replaceAll("[^a-zA-Z0-9 ]+", " ").toLowerCase();
	//line = value.toString().replaceAll("\t", " ");
	//line = line.trim();
	StringTokenizer itr = new StringTokenizer(contents," \t");
       while (itr.hasMoreTokens()) {
		String s= itr.nextToken();
		s.trim();
			if(s.length()>min_cpw)
			{
			token.set(s);
		 	context.write(token, docID);}
		}
	}
		//context.write(docID, new Text(contents));
      
  }
}

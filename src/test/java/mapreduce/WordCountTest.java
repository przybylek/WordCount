package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;


public class WordCountTest {

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
	public void setUp() throws Exception {
	    WordCount.WordCountMap mapper = new WordCount.WordCountMap();
	    WordCount.WordCountReduce reducer = new WordCount.WordCountReduce();
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		String input = "Adam ma kota i psa. Ala ma psa. Ola ma kota.";
		mapDriver.withInput(new LongWritable(), new Text(input));
		mapDriver.withOutput(new Text("Adam"), new IntWritable(1));
		mapDriver.withOutput(new Text("ma"), new IntWritable(1));
		mapDriver.withOutput(new Text("kota"), new IntWritable(1));
		mapDriver.withOutput(new Text("i"), new IntWritable(1));
		mapDriver.withOutput(new Text("psa"), new IntWritable(1));
		mapDriver.withOutput(new Text("Ala"), new IntWritable(1));
		mapDriver.withOutput(new Text("ma"), new IntWritable(1));
		mapDriver.withOutput(new Text("psa"), new IntWritable(1));
		mapDriver.withOutput(new Text("Ola"), new IntWritable(1));
		mapDriver.withOutput(new Text("ma"), new IntWritable(1));
		mapDriver.withOutput(new Text("kota"), new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
	  List<IntWritable> values = new ArrayList<IntWritable>();
	  values.add(new IntWritable(1));
	  values.add(new IntWritable(1));
	  reduceDriver.withInput(new Text("kota"), values);
	  reduceDriver.withOutput(new Text("kota"), new IntWritable(2));
	  reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException {
		String input = "Adam ma kota i psa. Ala ma psa. Ola ma kota.";
		mapReduceDriver.withInput(new LongWritable(), new Text(input));
		mapReduceDriver.withOutput(new Text("Adam"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("Ala"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("Ola"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("i"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("kota"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("ma"), new IntWritable(3));
		mapReduceDriver.withOutput(new Text("psa"), new IntWritable(2));
		mapReduceDriver.runTest();
	}
}
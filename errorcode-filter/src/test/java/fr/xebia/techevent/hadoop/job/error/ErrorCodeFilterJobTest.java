package fr.xebia.techevent.hadoop.job.error;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class ErrorCodeFilterJobTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        Configuration conf = new Configuration();
        conf.set("ERROR_CODE", "404");
        mapDriver = MapDriver.newMapDriver(new SearchCodeMapper())
                .withConfiguration(conf);
        reduceDriver = ReduceDriver.newReduceDriver(new CountReducer());
    }

    @Test
    public void request_with_404_should_count_1() {
        mapDriver
                .withInput(
                        new LongWritable(1),
                        new Text("78.236.167.225 - - [09/mai/2011:12:56:19 +0200] \"GET /non-existing.html HTTP/1.1\" 404 505"))
                .withOutput(new Text("/non-existing.html"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void request_other_than_error_should_return_nothing() {
        mapDriver
                .withInput(
                        new LongWritable(1),
                        new Text("78.236.167.225 - - [09/mai/2011:12:56:19 +0200] \"GET /existing.html HTTP/1.1\" 200 505"))
                .runTest();
    }

    @Test
    public void mapped_request_should_be_count() {
        reduceDriver
                .withInput(
                        new Text("/non-existing.html"),
                        Arrays.asList(new IntWritable[] { new IntWritable(1), new IntWritable(1) }))
                .withOutput(new Text("/non-existing.html"), new IntWritable(2))
                .runTest();
    }

}

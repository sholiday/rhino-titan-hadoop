package rhino.etl.hadoop.graph.insert;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EdgeInsertJob extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    conf.setBoolean("mapred.compress.map.output", true);
    conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    conf.set("mapred.max.split.size", "10200000");

    conf.set("titan.test", "false");

    args = new GenericOptionsParser(conf, args).getRemainingArgs();

    Job job = new Job(conf, "EdgeInsertJob" + conf.get("titanHost") + "-" + conf.get("edgeCutoff"));
    job.setMapSpeculativeExecution(false);
    job.setNumReduceTasks(0);

    // Input
    job.setInputFormatClass(SequenceFileInputFormat.class);

    // Output
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
    SequenceFileOutputFormat.setCompressOutput(job, true);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setMapperClass(EdgeInsertMapper.class);

    job.setJarByClass(EdgeInsertJob.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.out.println("Titan host = " + conf.get("titanHost"));

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new EdgeInsertJob(), args);
    System.exit(exitCode);
  }
}

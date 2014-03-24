package rhino.etl.hadoop.graph.insert;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import rhino.etl.hadoop.graph.writable.*;

public class VertexInsertJob extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    System.out.println("Running b1111atch!");
    Configuration conf = getConf();
    conf.setBoolean("mapred.compress.map.output", true);
    conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    conf.set("mapred.max.split.size", "10200000");

    conf.set("titan.test", "false");

    args = new GenericOptionsParser(conf, args).getRemainingArgs();

    Job job = new Job(conf, "VertexInsertJob-"+conf.get("titanHost")+"-"+conf.get("edgeCutoff"));
    job.setMapSpeculativeExecution(false);

    // Input
    job.setInputFormatClass(SequenceFileInputFormat.class);

    // Output
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
    SequenceFileOutputFormat.setCompressOutput(job, true);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(RhinoIdWritable.class);
    job.setMapOutputValueClass(TEdgeOrTitanIdWritable.class);

    job.setMapperClass(VertexInsertMapper.class);
    job.setReducerClass(VertexInsertReducer.class);

    job.setPartitionerClass(RhinoIdPartitioner.class);
    job.setGroupingComparatorClass(RhinoIdGroupingComparator.class);
    job.setSortComparatorClass(RhinoIdCompositeGroupingComparator.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(TEdgeWritable.class);

    job.setJarByClass(VertexInsertJob.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.out.println("Titan host = " + conf.get("titanHost"));

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new VertexInsertJob(), args);
    System.exit(exitCode);
  }
}

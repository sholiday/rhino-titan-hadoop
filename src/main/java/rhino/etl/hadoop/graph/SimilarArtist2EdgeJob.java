package rhino.etl.hadoop.graph;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatBaseInputFormat;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import rhino.etl.hadoop.graph.thrift.Item;
import rhino.etl.hadoop.graph.thrift.TEdge;
import rhino.etl.hadoop.graph.thrift.TVertex;
import rhino.etl.hadoop.graph.writable.TVertexWritable;

import java.io.IOException;

public class SimilarArtist2EdgeJob extends Configured implements Tool {
  public static class Map
      extends Mapper<WritableComparable, HCatRecord, LongWritable, TVertexWritable> {
    HCatSchema schema;

    public void setup(final Context context) throws IOException, InterruptedException {
      schema = HCatBaseInputFormat.getTableSchema(context);
    }

    protected void map(
        WritableComparable key,
        HCatRecord value,
        Context context)
        throws IOException, InterruptedException {

      final Integer fromId = value.getInteger("from_id", schema);
      final Integer toId = value.getInteger("to_id", schema);
      final Integer score = value.getInteger("score", schema);

      if (fromId == null) {
        throw new RuntimeException("fromId == null");
      }
      if (toId == null) {
        throw new RuntimeException("toId == null");
      }
      if (score == null) {
        throw new RuntimeException("score == null");
      }

      TVertex startV = new TVertex();
      startV.setRhinoId(fromId);

      TEdge edge = new TEdge();
      edge.setLabel("similarArtist");
      edge.setRightRhinoId(toId);
      edge.putToProperties("similarArtistScore", Item.int_value(score));
      startV.addToOutEdges(edge);

      context.write(new LongWritable(fromId), new TVertexWritable(startV));
    }
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    conf.setBoolean("mapred.compress.map.output", true);
    conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");

    args = new GenericOptionsParser(conf, args).getRemainingArgs();

    Job job = new Job(conf, this.getClass().getSimpleName());
    job.setNumReduceTasks(0);

    // Input
    HCatInputFormat.setInput(job, InputJobInfo.create("graph", "similar_artist", null));
    job.setInputFormatClass(HCatInputFormat.class);

    // Output
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
    SequenceFileOutputFormat.setCompressOutput(job, true);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(TVertexWritable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(TVertexWritable.class);

    job.setJarByClass(SimilarArtist2EdgeJob.class);
    job.setMapperClass(Map.class);

    FileOutputFormat.setOutputPath(job, new Path(args[0]));

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SimilarArtist2EdgeJob(), args);
    System.exit(exitCode);
  }
}

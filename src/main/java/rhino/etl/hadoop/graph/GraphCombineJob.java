package rhino.etl.hadoop.graph;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import rhino.etl.hadoop.graph.thrift.Item;
import rhino.etl.hadoop.graph.thrift.TEdge;
import rhino.etl.hadoop.graph.thrift.TVertex;
import rhino.etl.hadoop.graph.writable.TVertexWritable;

import java.io.IOException;
import java.util.*;

public class GraphCombineJob extends Configured implements Tool {
  public static class Map
      extends Mapper<LongWritable, TVertexWritable, LongWritable, TVertexWritable> {

    protected void map(LongWritable key, TVertexWritable tVertex,
                       Context context)
        throws IOException, InterruptedException {
      context.write(key, tVertex);
    }
  }

  public static class Reduce extends Reducer<LongWritable, TVertexWritable, LongWritable, TVertexWritable> {
    public enum Counters {
      VERTICES_PRODUCED,
      IN_EDGES_PRODUCED,
      OUT_EDGES_PRODUCED,
      VERTICES_WITH_NO_EDGES,
      PROPERTIES,
      MORE_THAN_ONE_EDGE,
      VERTICES_WITH_NO_PROPS,
      VERTICES_WITH_DROPPED_EDGES,
      DROPPED_EDGES
    }

    protected long maxMergeSoFar = 0;
    protected long maxEdgeSoFar = 0;

    protected long edgeCutoff = -1;

    @Override
    public void setup(final Reducer.Context context) throws IOException, InterruptedException {
      edgeCutoff = context.getConfiguration().getLong("edgeCutoff", -1);
    }

    @Override
    public void reduce(final LongWritable key, final Iterable<TVertexWritable> values,
                       final Reducer<LongWritable, TVertexWritable, LongWritable, TVertexWritable>.Context context)
        throws IOException, InterruptedException {
      TVertex tVertex = new TVertex();
      tVertex.setRhinoId(key.get());

      tVertex.setProperties(new HashMap<String, Item>());
      tVertex.setInEdges(new ArrayList<TEdge>());
      tVertex.setOutEdges(new ArrayList<TEdge>());

      long mergeThisTime = 0;
      for (TVertexWritable valWritable : values) {
        mergeThisTime++;

        if (edgeCutoff > 0 && mergeThisTime > edgeCutoff) {
          continue;
        }

        TVertex val = valWritable.get().deepCopy();
        // Copy properties.
        if (val.isSetProperties()) {
          tVertex.properties.putAll(val.getProperties());
          context.getCounter(Counters.PROPERTIES).increment(val.getProperties().size());
        }

        if (val.isSetInEdges()) {
          tVertex.inEdges.addAll(val.inEdges);
          context.getCounter(Counters.IN_EDGES_PRODUCED).increment(val.inEdges.size());
        }

        if (val.isSetOutEdges()) {
          tVertex.outEdges.addAll(val.outEdges);
          context.getCounter(Counters.OUT_EDGES_PRODUCED).increment(val.outEdges.size());
          if (val.outEdges.size() == 0) {
            context.getCounter(Counters.VERTICES_WITH_NO_EDGES).increment(1l);
          }
          if (val.outEdges.size() > 1) {
            context.getCounter(Counters.MORE_THAN_ONE_EDGE).increment(1l);
          }
        } else {
          context.getCounter(Counters.VERTICES_WITH_NO_EDGES).increment(1l);
        }
      }
      if (mergeThisTime > maxMergeSoFar) {
        maxMergeSoFar = mergeThisTime;
        System.out.println("maxMergeSoFar = " + maxMergeSoFar);
      }
      if (tVertex.getOutEdgesSize() > maxEdgeSoFar) {
        maxEdgeSoFar = tVertex.getOutEdgesSize();
        System.out.println("maxEdgeSoFar = " + maxEdgeSoFar);
        for (java.util.Map.Entry<String, Item> entry: tVertex.properties.entrySet()) {
          System.out.println(entry.getKey() + " = " + entry.getValue());
        }
      }

      if (edgeCutoff > 0 && mergeThisTime > edgeCutoff) {
        context.getCounter(Counters.VERTICES_WITH_DROPPED_EDGES).increment(1L);
        context.getCounter(Counters.DROPPED_EDGES).increment(mergeThisTime - edgeCutoff);

        System.out.println("---");
        for (java.util.Map.Entry<String, Item> entry: tVertex.properties.entrySet()) {
          System.out.println(entry.getKey() + " = " + entry.getValue());
        }
        System.out.println("Dropped " + (mergeThisTime - edgeCutoff) + " edges.");
        System.out.println("---");
      }

      if (tVertex.properties.size() == 0) {
        context.getCounter(Counters.VERTICES_WITH_NO_PROPS).increment(1L);
      } else {
        context.write(key, new TVertexWritable(tVertex));
        context.getCounter(Counters.VERTICES_PRODUCED).increment(1L);
      }
    }
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    conf.setBoolean("mapred.compress.map.output", true);
    conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");


    args = new GenericOptionsParser(conf, args).getRemainingArgs();

    Job job = new Job(conf, "GraphCombineJob-"+conf.get("edgeCutoff"));

    // Input
    job.setInputFormatClass(SequenceFileInputFormat.class);

    // Output
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
    SequenceFileOutputFormat.setCompressOutput(job, true);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(TVertexWritable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(TVertexWritable.class);

    job.setJarByClass(GraphCombineJob.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);


    FileOutputFormat.setOutputPath(job, new Path(args[0]));
    for (int i = 1; i < args.length; i++) {
      FileInputFormat.addInputPath(job, new Path(args[i]));
    }

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new GraphCombineJob(), args);
    System.exit(exitCode);
  }
}

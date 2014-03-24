package rhino.etl.hadoop.graph;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import junit.framework.TestCase;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import rhino.etl.hadoop.graph.thrift.Item;
import rhino.etl.hadoop.graph.thrift.TEdge;
import rhino.etl.hadoop.graph.thrift.TVertex;
import rhino.etl.hadoop.graph.writable.TVertexWritable;

import java.io.IOException;
import java.util.*;

@RunWith(PowerMockRunner.class)
public class GraphCombineTest extends TestCase {
  private GraphCombineJob.Reduce reducer;
  private ReduceDriver driver;

  private TVertex simpleVertex(Long id, String key, String value) {
    TVertex v = new TVertex();
    v.setRhinoId(id);
    v.putToProperties(key, Item.string_value(value));
    return v;
  }

  public void setUp() {
    reducer = new GraphCombineJob.Reduce();
    driver = new ReduceDriver(reducer);
  }

  public void testCombineProperties() throws IOException {
    List<TVertexWritable> values = new ArrayList<TVertexWritable>();
    List<TVertex> values2 = new ArrayList<TVertex>();
    values.add(new TVertexWritable(simpleVertex(123L, "some", "thing")));
    values.add(new TVertexWritable(simpleVertex(123L, "another", "otherThing")));

    values2.add(simpleVertex(123L, "some", "thing"));
    values2.add(simpleVertex(123L, "another", "otherThing"));

    TVertex expected = new TVertex();
    expected.setRhinoId(123L);

    expected.putToProperties("some", Item.string_value("thing"));
    expected.putToProperties("another", Item.string_value("otherThing"));

    List<Pair<LongWritable, TVertexWritable>> result = driver.withInput(new LongWritable(123),
        values).run();
    assertEquals(1, result.size());

    TVertex actual = result.get(0).getSecond().get();
    assertEquals(2, actual.properties.size());
    assertEquals("thing", actual.properties.get("some").getString_value());
    assertEquals("otherThing", actual.properties.get("another").getString_value());
  }

  public void testCombineEdges() throws IOException {
    List<TVertexWritable> values = new ArrayList<TVertexWritable>();

    TVertex v1a = new TVertex();
    v1a.setRhinoId(1123);
    v1a.putToProperties("some1", Item.string_value("thing1"));
    TVertex v1b = new TVertex();
    v1b.setRhinoId(2123);
    TVertex v1c = new TVertex();
    v1c.setRhinoId(3123);
    TVertex v1d = new TVertex();
    v1d.setRhinoId(4124);
//    v1d.putToProperties("some", Item.string_value("thing"));

    {
      TEdge e = new TEdge();
      e.setRightRhinoId(124);
      e.setLabel("someEdge");
      e.putToProperties("someProperty", Item.short_value((short)5));
      v1a.addToOutEdges(e);
    }

    {
      TEdge e = new TEdge();
      e.setRightRhinoId(124);
      e.setLabel("someEdge2");
      v1b.addToOutEdges(e);
    }

    {
      TEdge e = new TEdge();
      e.setRightRhinoId(125);
      e.setLabel("someEdge3");
      v1c.addToOutEdges(e);
    }

    values.add(new TVertexWritable(v1a.deepCopy()));
    values.add(new TVertexWritable(v1b.deepCopy()));
    values.add(new TVertexWritable(v1c.deepCopy()));
    values.add(new TVertexWritable(v1d.deepCopy()));

    assertEquals(4, values.size());
    List<Pair<LongWritable, TVertexWritable>> result = driver.withInput(new LongWritable(123), values).run();
    assertEquals(1, result.size());
    TVertex actual = result.get(0).getSecond().get();

    assertEquals(3, actual.getOutEdgesSize());
    assertEquals(0, actual.getInEdgesSize());
  }
}

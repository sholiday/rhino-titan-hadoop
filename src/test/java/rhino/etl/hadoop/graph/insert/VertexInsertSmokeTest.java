package rhino.etl.hadoop.graph.insert;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import junit.framework.TestCase;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import rhino.etl.hadoop.graph.thrift.Item;
import rhino.etl.hadoop.graph.thrift.TEdge;
import rhino.etl.hadoop.graph.thrift.TVertex;
import rhino.etl.hadoop.graph.writable.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
public class VertexInsertSmokeTest extends TestCase {
  private Mapper<LongWritable, TVertexWritable, RhinoIdWritable, TEdgeOrTitanIdWritable> mapper;
  private Reducer<RhinoIdWritable, TEdgeOrTitanIdWritable, LongWritable, TEdgeWritable> reducer;
  private MapReduceDriver<LongWritable, TVertexWritable, RhinoIdWritable, TEdgeOrTitanIdWritable,
      LongWritable, TEdgeWritable> driver;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    conf.setBoolean("titan.test", true);
    conf.set("titan.storage.directory", folder.getRoot().getAbsolutePath());

    mapper = new VertexInsertMapper();
    reducer = new VertexInsertReducer();

    driver = new MapReduceDriver<LongWritable, TVertexWritable, RhinoIdWritable,
        TEdgeOrTitanIdWritable, LongWritable, TEdgeWritable>(mapper, reducer)
        .withConfiguration(conf);
    driver.setKeyGroupingComparator(new RhinoIdGroupingComparator());
    driver.setKeyOrderComparator(new RhinoIdCompositeGroupingComparator());
  }

  public void testSmoke() throws IOException {
    Long v0RhinoId = 100L;
    TVertex v0 = new TVertex();
    {
      v0.setRhinoId(v0RhinoId);
      v0.putToProperties("artistName", Item.string_value("The Beatles"));

      TEdge e = new TEdge();
      e.setRightRhinoId(200L);
      e.setLabel("groupArtist");
      v0.addToOutEdges(e);
    }

    Long v1RhinoId = 200L;
    TVertex v1 = new TVertex();
    {
      v1.setRhinoId(v1RhinoId);
      v1.putToProperties("artistName", Item.string_value("John Lennon"));
    }

    Long v2RhinoId = 300L;
    TVertex v2 = new TVertex();
    {
      v2.setRhinoId(v2RhinoId);
      v2.putToProperties("artistName", Item.string_value("The Plastic Ono Band"));

      TEdge e1 = new TEdge();
      e1.setRightRhinoId(200L);
      e1.setLabel("groupArtist");
      v2.addToOutEdges(e1);

      TEdge e2 = new TEdge();
      e2.setRightRhinoId(200L);
      e2.setLabel("groupArtist");
      v2.addToOutEdges(e2);
    }

    Long v3RhinoId = 400L;
    TVertex v3 = new TVertex();
    {
      v3.setRhinoId(v3RhinoId);
      v3.putToProperties("artistName", Item.string_value("Yoko Ono"));
    }

    driver.withInput(new LongWritable(v0RhinoId), new TVertexWritable(v0))
          .withInput(new LongWritable(v1RhinoId), new TVertexWritable(v1))
          .withInput(new LongWritable(v2RhinoId), new TVertexWritable(v2))
          .withInput(new LongWritable(v3RhinoId), new TVertexWritable(v3)).run();

    TitanGraph g = getGraph();
    Map<Long, Vertex> v = getVertexMap(g);

    assertTrue(v.containsKey(v0RhinoId));
    assertTrue(v.containsKey(v1RhinoId));

    assertEquals("The number of vertices does not match.", 4, v.size());

    assertEquals("The Beatles", v.get(v0RhinoId).getProperty("artistName"));

    assertEquals("John Lennon", v.get(v1RhinoId).getProperty("artistName"));

    g.shutdown();
  }

  public TitanGraph getGraph() {
    org.apache.commons.configuration.Configuration conf = new BaseConfiguration();
    conf.setProperty("storage.directory", folder.getRoot().getAbsolutePath());
    return TitanFactory.open(conf);
  }

  public List<Vertex> getVerticies(TitanGraph graph) {
    List<Vertex> verticies = new ArrayList<Vertex>();
    for (Vertex vv : graph.getVertices()) {
      verticies.add(vv);
    }

    return verticies;
  }

  public Map<Long, Vertex> getVertexMap(TitanGraph g) {
    List<Vertex> verticies = getVerticies(g);

    Map<Long, Vertex> map = new HashMap<Long, Vertex>();
    for (Vertex v : verticies) {
      map.put((Long) v.getProperty("rhinoId"), v);
    }

    return map;
  }
}

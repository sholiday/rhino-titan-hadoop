namespace java rhino.etl.hadoop.graph.thrift

union Item {
  1: i16 short_value;
  2: i32 int_value;
  3: i64 long_value;
  4: double double_value;
  5: string string_value;
  6: binary bytes_value;
}

struct TEdge {
    1: optional i64 leftRhinoId,
    2: optional i64 leftTitanId,
    3: optional i64 rightRhinoId,
    4: optional i64 rightTitanId,
    5: optional string label,
    6: optional map<string, Item> properties
}

struct TVertex {
  1: optional i64 rhinoId,
  2: optional i64 titanId,
  3: optional map<string, Item> properties,
  4: optional list<TEdge> outEdges,
  5: optional list<TEdge> inEdges
}

union TEdgeOrTitanId {
  1: i64 titanId;
  2: TEdge tEdge;
}
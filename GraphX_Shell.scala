import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val vertices=Array((1L, ("SFO")),(2L, ("ORD")),(3L,("DFW")))
val vRDD= sc.parallelize(vertices)
vRDD.take(1)

val edges = Array(Edge(1L,2L,1800),Edge(2L,3L,800),Edge(3L,1L,1400))
val eRDD= sc.parallelize(edges)
eRDD.take(2)

val graph = Graph(vRDD, eRDD)

graph.vertices.take(3).foreach(println)
//(1,SFO)
//(2,ORD)
//(3,DFW)

graph.edges.take(3).foreach(println)
//Edge(1,2,1800)
//Edge(2,3,800)
//Edge(3,1,1400)

graph.triplets.take(3).foreach(println)
//((1,SFO),(2,ORD),1800)
//((2,ORD),(3,DFW),800)
//((3,DFW),(1,SFO),1400)

//Có bao nhiêu airport?
val numairports = graph.numVertices

//Có bao nhiêu routes?
val numroutes = graph.numEdges

//Các routes có khoảng cách > 1000?
graph.edges.filter {
  case Edge(src, dst, distance) => distance > 1000}.collect.foreach(println)
//Edge(1,2,1800)
//Edge(3,1,1400)

//Sắp xếp và in ra các tuyến đường dài nhất?
graph.triplets.sortBy(_.attr, ascending=false).map(triplet =>
  "Distance " + triplet.attr.toString + " from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").collect.foreach(println)
//Distance 1800 from SFO to ORD.
//Distance 1400 from DFW to SFO.
//Distance 800 from ORD to DFW.






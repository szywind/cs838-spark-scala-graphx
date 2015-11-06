/**
 * Created by Zhenyuan Shen 2015/11/05
 */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import SparkContext._ 
import scala.util.control.Breaks._


val textFile = sc.textFile("QuestionA2_countdata_1")
val nVertex = textFile.count().toInt

val terms = textFile.map(line => line.split(" "))
val VD = terms.zipWithIndex().map(line => (line._2, line._1)) //val id = sc.parallelize(List.range(0, nVertex))
val VDarray = VD.collect()

def findEdges(s: (Long, Array[String])): ArrayBuffer[Edge[Int]] = {
	val edges = ArrayBuffer[Edge[Int]]()
    //for (i <- 0 to nVertex-1){ /* i-th vertex  [BUG] self loop is not considered as an edge */
    for (i <- s._1+1 to nVertex-1){ 
    	val temp = VDarray(i.toInt)
    	breakable{ for (j <- 0 to s._2.length-1){
    		if( temp._2 contains (s._2(j)) ){
    		//if( (temp._2 contains (s._2(j))) && (temp._1 != s._1)){
    			//edges :+ (s._1, temp._1, s._2.size - temp._2.size)
    			edges += Edge(s._1, temp._1, s._2.size - temp._2.size)
    			break
    		}
    	}}
    	//println(edges.length)
    }  
    edges  
}

val ED = VD.flatMap(line => findEdges(line))

// Create vertex table
val vTable: RDD[(VertexId, Array[String])] = VD //vTable.map(line => line._1).collect()
// Create edge table
val eTable: RDD[Edge[Int]] = ED
// Construct the direct graph (with 43014 edges in total, but only store 21507 edges)
val graph = Graph(vTable, eTable)

/*
 * Question 1.   Find the number of edges where the number of words in the source vertex is strictly larger than the number of words in the destination vertex.
 * Hint: to solve this question please refer to the Property Graph examples from here.
 */

// Count all edges where the src and des have different numbers of words
graph.edges.filter(e => e.attr != 0 ).count // return; 21422

/*
 * Question 2.   Find the most popular vertex. A vertex is the most popular if it has the most number of edges to his neighbors and it has the maximum number of words.
 * Hint: you can ignore the frequency of the words for this question.

 * Question 3.   Find the average number of words in every neighbor of a vertex.
 * Hint: to solve questions 2 and 3 please refer to the Neighborhood Aggregation examples from here.
 */
val wordCount = vTable.map(x => (x._1, x._2.length))
val wcGraph = Graph(wordCount, eTable)

val neighbor: VertexRDD[(Int, Int)] = wcGraph.aggregateMessages[(Int, Int)](
  triplet => { // Map Function
    {
      assert(triplet.srcId < triplet.dstId)
      triplet.sendToDst(1, triplet.srcAttr)
      triplet.sendToSrc(1, triplet.dstAttr)
    }
  },
  // Add counter and age
  (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
)
// Divide total age by number of older followers to get average age of older followers
val avg: VertexRDD[Double] =
  neighbor.mapValues( (id, value) => value match { case (count, totalWords) => totalWords / count.toDouble } )

// Find the most popular vertex as Question 2
neighbor.join(wordCount).map( x => (x._2._1._1, (x._2._2, x._1))).sortByKey(false).map(x => x._2).sortByKey(false).take(1) // return: Array((318,36)) 36-th sample

// Display the results of Qustion 3
avg.collect.foreach(println(_))



/*
 * Question 4.   Find the most popular word across all the tweets.
 */

val wc = terms.flatMap(line => line)
val mostPopularVertex = wc.map(word => (word, 1)).reduceByKey((a,b) => a+b).map(x => (x._2, x._1)).sortByKey(false).take(1)(0)._2 // return: String "new"


/*
 * Question 6.   Find the number of time intervals which have the most popular word.
 */
 graph.vertices.filter(v => v._2 contains mostPopularVertex).count // return: 189







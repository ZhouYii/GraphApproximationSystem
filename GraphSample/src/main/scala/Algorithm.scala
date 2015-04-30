package org.template.vanilla

import org.apache.spark.graphx._

import io.prediction.controller.PAlgorithm
import io.prediction.controller.Params

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashSet

import grizzled.slf4j.Logger

case class AlgorithmParams(mult: Int) extends Params

class Algorithm(val ap: AlgorithmParams)
  // extends PAlgorithm if Model contains RDD[]
  extends PAlgorithm[Int, Model, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: Int ): Model = {
    new Model(4, sc)
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def predict(model: Model, query: Query): PredictedResult = {
    // Load Graph
    val g = loadGraph(model.sc, query.queryAlgo, query.graphDataPath)

    val elapsed = time {
    // Get Samples
    val samples = getSamples(g,
      query.sampleType,
      query.sampleFrac,
      query.stitchNumGraph)

    // Run query on samples
    val aggregateResult = runQuery(samples, query.queryAlgo, query.stitchStrategy)

    val groundTruth = runQuery(List(g), query.queryAlgo, "None")

    // PageRank
    runEval[Graph[Double,Double]](
        aggregateResult.asInstanceOf[Graph[Double,Double]],
        groundTruth.asInstanceOf[Graph[Double,Double]],
        query)
    }

    // Test double-to-string
    PredictedResult(0.54.toString)
  }

  def loadGraph(sc: SparkContext, queryAlgo: String, graphDataPath: String) = {
    queryAlgo match {
      case "triangleCount" => 
        GraphLoader.edgeListFile(sc, graphDataPath, canonicalOrientation=true)
      case _ =>
        GraphLoader.edgeListFile(sc, graphDataPath)
    }
  }

  def runEval[T](aggregateResult : T, groundTruth : T, query : Query) {
    // Params
    val numSamples = query.stitchNumGraph
    val sampleType = query.sampleType
    val sampleFrac = query.sampleFrac
    val queryAlgo = query.queryAlgo
    val stitchStrategy = query.stitchStrategy

    queryAlgo match {
      case "pageRank" => {
        val result = aggregateResult.asInstanceOf[Graph[Double,Double]]
        val truth = groundTruth.asInstanceOf[Graph[Double,Double]]

        var metricName = "Edit Distance"
        var metricScore = Metrics.pageRankEditMetric(truth, result)
        println(s"$sampleType,$sampleFrac,$queryAlgo,$metricName,$metricScore,$numSamples,$stitchStrategy")

        metricName = "Intersection"
        metricScore = Metrics.pageRankIntersectMetric(truth, result)
        println(s"$sampleType,$sampleFrac,$queryAlgo,$metricName,$metricScore,$numSamples,$stitchStrategy")
      }
    }

  }

  def runStitching[T](results: List[T],
    queryAlgo: String,
    stitchStrategy: String) : T = {
    
    queryAlgo match {
      case "pageRank" => 
        stitchStrategy match {
          case _ => results.apply(0).asInstanceOf[T]
        }
    }
  }

  def runQuery(samples: List[Graph[Int,Int]],
    queryAlgo: String,
    stitchStrategy: String) = {

    queryAlgo match {
      case "triangleCount" => {
        val results = samples.map(g => g.triangleCount())
        runStitching[Graph[Int,Int]](results, queryAlgo, "None")
      }

      case "pageRank" => {
        val results = samples.map(g => g.pageRank(0.001))
        runStitching[Graph[Double,Double]](results, queryAlgo, "None")
        // vertex, score
        // result.apply(0).vertices.foreach(println)
      }

      case "connectedComponents" => {
        val result = samples.map(g => g.connectedComponents())
        result

        //val result = samples.map(g => ccGetNumComponents(g.connectedComponents()))

        // stitching can be good. if a,b are in different components in one
        // sample, and a,b are in sample component in a different sample, then
        // you can infer a,b are in same sample

      }

    }

  }

  def getSamples(g: Graph[Int,Int],
    sampleType: String,
    sampleFrac: Double,
    numSamples: Int) = {

    val numList = List.range(0, numSamples)
    sampleType match {
      /*
      case "layered" => {

      }
      */

      case "Edge Sample" => {
        numList.map(x => SamplingAlgo.EdgeSample(g, sampleFrac))
      }

      case _ => {
        numList.map(x => g)
      }
    }
  }

  def ccGetNumComponents(g : Graph[VertexId,Int]) = {
    val hs = new scala.collection.mutable.HashSet
    val ids = g.vertices.map(x => x._2).distinct
    ids.count()
  }
}

class Model(val mc: Int, val sc: SparkContext) extends Serializable

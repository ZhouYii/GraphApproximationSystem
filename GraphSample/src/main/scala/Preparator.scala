package org.template.vanilla

import io.prediction.controller.PPreparator
import io.prediction.data.storage.Event

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[Int , Int] {

  def prepare(sc: SparkContext, trainingData: Int) : Int = {
    0
  }
}

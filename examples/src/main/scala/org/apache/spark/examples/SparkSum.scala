/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

/** Usage: SparkSum */
object SparkSum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSum")
      .getOrCreate()
    val sc = spark.sparkContext

    println("Waiting for executors to join")
    Thread.sleep(5000)

    val trials = if (args.length > 0) args(0).toInt else 5
    val iterations = if (args.length > 1) args(1).toInt else 10
    val mappers = if (args.length > 2) args(2).toInt else 32
    val reducers = if (args.length > 3) args(3).toInt else 32

    (0 until trials).foreach { trial =>
      val rdd = sc.parallelize(1L to mappers * 100L, mappers).cache()
      val partitioner = new HashPartitioner(reducers)
      var stateRDD = sc.parallelize(0 until reducers, reducers).map(x => (x, 0L)).cache()

      val batchRDDs = (0 until iterations).map { i =>
        val dataRdd = rdd.map { x =>
          val inc = x + i * (100 * mappers)
          (inc % reducers, inc)
        }
        stateRDD = dataRdd.groupByKey(partitioner).zip(stateRDD).map {
          y => (y._2._1, y._2._2 + y._1._2.sum)
        }
        stateRDD.cache()
        stateRDD
      }

      val pairCollectFunc = (iter: Iterator[(Int, Long)]) => {
        iter.map(i => (i._1, i._2)).toArray
      }

      (0 until iterations).foreach { i =>
        val begin = System.nanoTime()
        sc.runJob(batchRDDs(i), pairCollectFunc)
        val total = System.nanoTime() - begin
        println("Iteration-" + trial + "-" + i + "-" + (total/1e6) + "-ms")
      }
    }
    sc.stop()
  }
}
// scalastyle:on println

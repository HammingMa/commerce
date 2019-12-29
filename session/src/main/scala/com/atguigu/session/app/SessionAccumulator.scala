package com.atguigu.session.app

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SessionAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Int]]{

  val countMap  = new mutable.HashMap[String,Int]()

  override def isZero: Boolean = {
    countMap.isEmpty
  }

  override def copy(): AccumulatorV2[String,mutable.HashMap[String,Int]] = {

    val acc = new SessionAccumulator()

    acc.countMap ++= countMap

    acc
  }

  override def reset(): Unit = {
    countMap.clear()
  }

  override def add(key: String): Unit = {
    if(!countMap.contains(key)){
      countMap.put(key,0)
    }

    countMap.update(key,countMap(key)+1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {

    other match {
      case acc:SessionAccumulator =>{
        acc.countMap.foldLeft(this.countMap){
          case (map,(key,value)) => {
            map += (key -> (map.getOrElse(key,0)+value))
          }
        }
      }

    }
  }


  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }

}

package com.nxhuy.spark.streaming.VeloLyonSparkStreaming

import org.apache.http._
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder

import java.io.InputStreamReader
import java.io.BufferedReader

import java.util.{Calendar, Properties, UUID}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random
import scala.util.control.Breaks
import scala.util

object VeloLyon {
  val url = "https://api.jcdecaux.com/vls/v1/stations?contract=lyon&apiKey="
  val keyAPI = "d525e43ff58fff3ec1dd3c1b6a61f738fecce5c8"
  
  def main(args: Array[String]): Unit = {
    
    val brokers = util.Try(args(0)).getOrElse("localhost:9092")
    val topic = util.Try(args(1)).getOrElse("velo-lyon")
    val events = util.Try(args(2)).getOrElse("0").toInt
    val internalEvent = util.Try(args(3)).getOrElse("30").toInt
    val rndStart = util.Try(args(4)).getOrElse("0").toInt
    val rndEnd = util.Try(args(5)).getOrElse("500").toInt
    val clientId = UUID.randomUUID().toString()
    
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", clientId)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    val producer = new KafkaProducer[String, String](props)
    
    println("===================== BEGIN =====================")
    
    val loop = new Breaks()
    var i = 0
    loop.breakable{
      while(true) {
        val client = HttpClientBuilder.create.build
        val get = new HttpGet(url+keyAPI)
        get.addHeader("Accept", "application/json")
    
        val response = client.execute(get)
        val bufferedReader = new BufferedReader(new InputStreamReader(response.getEntity.getContent))
    
        val lines = bufferedReader.readLine()
        val key = UUID.randomUUID().toString().split("-")(0)
        
        val data = new ProducerRecord[String, String](topic, key, lines)
        
        println("---- topic: " + topic + " ----")
        println("key: " + key)
        println("value: " + lines.length())
        producer.send(data)
        
        
        if(internalEvent > 0)
          Thread.sleep(internalEvent * 1000)
          
      }
    }
    
  }
}
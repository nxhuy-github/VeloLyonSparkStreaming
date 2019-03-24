package com.nxhuy.spark.streaming.VeloLyonSparkStreaming

import org.apache.http._
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder

import java.io.InputStreamReader
import java.io.BufferedReader

object VeloLyon {
  val url = "https://api.jcdecaux.com/vls/v1/stations?contract=lyon&apiKey="
  val key = "d525e43ff58fff3ec1dd3c1b6a61f738fecce5c8"
  
  def main(args: Array[String]){
    val client = HttpClientBuilder.create.build
    val get = new HttpGet(url+key)
    get.addHeader("Accept", "application/json")
    
    val response = client.execute(get)
    val bufferedReader = new BufferedReader(new InputStreamReader(response.getEntity.getContent))
    
    val lines = bufferedReader.readLine()
    println(lines)
  }
}
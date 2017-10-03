package com.abhi

import java.io.ByteArrayOutputStream
import java.nio.file.Paths

import akka.stream.ActorMaterializer
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import com.sksamuel.avro4s.{AvroBinaryOutputStream, AvroOutputStream, AvroSchema}
import akka.stream.scaladsl._
import akka.util.ByteString
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import akka.kafka.scaladsl.Producer
import akka.stream.ClosedShape
import akka.stream.alpakka.file.scaladsl.FileTailSource

import scala.concurrent.duration._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by ASrivastava on 10/1/17.
  */
object KafkaAvroProducer extends App {
   implicit val actorSystem = ActorSystem()
   implicit val actorMaterializer = ActorMaterializer()
   val resource = getClass.getResource("/countrycapital.csv")
   val path = Paths.get(resource.toURI)
   val source = FileTailSource.lines(path, 8092, 100 millis).map(ByteString(_))
   val flow1 = CsvParsing.lineScanner()
   val flow2 = Flow[List[ByteString]].map(list => list.map(_.utf8String))
   val flow3 = Flow[List[String]].map(list => CountryCapital(list(0), list(1)))
   val flow4 = Flow[CountryCapital].map { cc =>
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[CountryCapital](baos)
      output.write(cc)
      output.close()
      val result = baos.toByteArray
      baos.close()
      result
   }
   val producerSettings = ProducerSettings(actorSystem, new ByteArraySerializer, new ByteArraySerializer)
      .withBootstrapServers("abhisheks-mini:9093")

   val flow5 = Flow[Array[Byte]].map{array =>
      print(array)
      new ProducerRecord[Array[Byte], Array[Byte]]("test", array)
   }
   val sink = Producer.plainSink(producerSettings)

   //val sink = Sink.foreach[CountryCapital](println)

   val graph = RunnableGraph.fromGraph(GraphDSL.create(sink){implicit builder =>
      s =>
         import GraphDSL.Implicits._
         //source ~> flow1 ~> flow2 ~> flow3 ~> flow4 ~> flow5 ~> flow6 ~> s.in
         source ~> flow1 ~> flow2 ~> flow3  ~> flow4 ~> flow5 ~> s.in
         ClosedShape
   })
   val future = graph.run()
   future.onComplete { _ =>
      actorSystem.terminate()
   }
   Await.result(actorSystem.whenTerminated, Duration.Inf)
   System.exit(0)
}

case class CountryCapital(country: String, capital: String)

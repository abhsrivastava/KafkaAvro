package com.abhi

import java.io.ByteArrayInputStream

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, ClosedShape}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl._
import com.sksamuel.avro4s.AvroInputStream
import akka.kafka.ConsumerMessage
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by ASrivastava on 10/2/17.
  */
object KafkaAvroConsumer extends App {
   implicit val actorSystem = ActorSystem()
   implicit val actorMaterializer = ActorMaterializer()
   val consumerSettings = ConsumerSettings(actorSystem, new ByteArrayDeserializer(), new ByteArrayDeserializer)
      .withBootstrapServers("abhisheks-mini:9093")
      .withGroupId("abhi")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
   val consumer = Consumer.committableSource(consumerSettings, Subscriptions.topics("test"))
   val flow1 = Flow[ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]]].map{ msg => msg.record.value()}
   val flow2 = Flow[Array[Byte]].map{ array =>
      val bais = new ByteArrayInputStream(array)
      val input = AvroInputStream.binary[CountryCapital](bais)
      input.iterator.toSeq.head
   }
   val sink = Sink.foreach[CountryCapital](println)
   val graph = RunnableGraph.fromGraph(GraphDSL.create(sink){ implicit builder =>
      s =>
         import GraphDSL.Implicits._
         consumer ~> flow1 ~> flow2 ~> s.in
         ClosedShape
   })
   val future = graph.run()
   future.onComplete { _ =>
      actorSystem.terminate()
   }
}

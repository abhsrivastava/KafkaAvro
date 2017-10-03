name := "KafkaAvro"

version := "1.0"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
   "com.sksamuel.avro4s" %% "avro4s-core" % "1.8.0",
   "com.typesafe.akka" %% "akka-stream" % "2.5.6",
   "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "0.13",
   "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.13",
   "com.typesafe.akka" %% "akka-stream-kafka" % "0.17"
)
package eci.edu.co
package deployements.kafka.consumers


import adapters.postgresql.PostgreSQL
import domain.Pollution
import domain.commands.SavePollution
import domain.ports.ForUpdatingRepository
import protos.PollutionCreated.PollutionCreated
import java.util.Properties
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._


object UpdateStreamPollutionConsumer extends App{

  println("Starting UpdateStreamPollutionConsumer")


  val kafkaStreamProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9093")
    props.put("application.id", "consumer-pollution-stream")
    props
  }

  val forUpdatingRepository: ForUpdatingRepository = PostgreSQL

  val streams = new KafkaStreams(streamTopology, kafkaStreamProps)
  Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))
  streams.start()

  def streamTopology = {
    val streamsBuilder = new StreamsBuilder()
    streamsBuilder
      .stream[String, Array[Byte]]("topic-pollution")
      .foreach(
        (key, value) => {
          println(s"Key: $key and value $value")
          val deserializedPollution = Pollution.fromPollutionCreated(PollutionCreated.parseFrom(value))
          println(s"Deserialized message: $deserializedPollution")
          SavePollution(forUpdatingRepository, deserializedPollution)
            .execute() match {
            case Right(t) => println(s"Pollution $t saved successfully")
            case Left(e) => println("There was an error storing the pollution", e)
          }
        }
      )
    streamsBuilder.build()
  }


}

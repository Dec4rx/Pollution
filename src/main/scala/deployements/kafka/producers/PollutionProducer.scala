package eci.edu.co
package deployements.kafka.producers

import domain.Pollution

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import java.util.Properties


object PollutionProducer extends App{

  println("Starting PollutionProducer")

  val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9093")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[ByteArraySerializer].getName)
    props
  }

  val producer = new KafkaProducer[String, Array[Byte]](kafkaProducerProps)

  while (true) {
    val pollutionCreated = Pollution.randomPollutionCreated()
    println(s"Message to be send: $pollutionCreated")
    println(pollutionCreated)
    val messageSent = producer.send(new ProducerRecord[String, Array[Byte]]("topic-pollution", pollutionCreated.aqiColor, pollutionCreated.toByteArray)).get()
    println(s"Message Sent - $messageSent")
  }
  producer.close()
}

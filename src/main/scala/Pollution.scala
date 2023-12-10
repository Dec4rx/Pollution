package eci.edu.co
package domain

import protos.PollutionCreated.PollutionCreated

import com.google.protobuf.timestamp.Timestamp

import scala.util.Random
import scala.io.Source
import play.api.libs.json._

import java.time.LocalDate
import java.util.Calendar



case class Pollution(state: String, city: String, aqiValue: Double, aqiColor: String, hour: String, date: String, created: java.sql.Timestamp) {

  def ToPollutionCreated: PollutionCreated = PollutionCreated(
    state,
    city,
    aqiValue,
    aqiColor,
    hour,
    date,
    java.util.UUID.randomUUID.toString,
    Some(new Timestamp(created.getTime))
  )

  override def toString: String = s"(state: $state, city: $city, aqiValue: $aqiValue, aqiColor: $aqiColor, hour: $hour, date: $date, created: $created)"
}

object Pollution{
  def fromPollutionCreated(p: PollutionCreated): Pollution = {
    Pollution(p.state, p.city, p.aqiValue, p.aqiColor, p.hour, p.date, new java.sql.Timestamp(p.created.get.seconds))
  }

  val green = "green"
  val yellow = "yellow"
  val orange = "orange"
  val red = "red"
  val purple = "purple"
  val maroon = "maroon"

  def randomPollutionCreated() = {
    val statesAndCities = readStatesAndCities("US_States_and_Cities.json")
    val (state, city) = getRandomStateAndCity(statesAndCities)
    val (aqiValue, aqiColor) = generateAqiValueAndColor()
    val randomTime = generateRandomTime()
    val randomDate = generateRandomDate(2019, 2022)
    Pollution(
      state = state,
      city = city,
      aqiValue = aqiValue,
      aqiColor = aqiColor,
      hour = randomTime,
      date = randomDate,
      created = new java.sql.Timestamp(Calendar.getInstance().getTimeInMillis)
    ).ToPollutionCreated
  }

  // Función para leer el archivo JSON desde los recursos y convertirlo en un mapa de estados a ciudades
  def readStatesAndCities(resourceName: String): Map[String, Seq[String]] = {
    val source = Source.fromResource(resourceName)
    val json: JsValue = Json.parse(source.mkString)
    source.close()
    json.as[Map[String, Seq[String]]]
  }

  // Función para generar un estado y una ciudad aleatorios
  def getRandomStateAndCity(statesAndCities: Map[String, Seq[String]]): (String, String) = {
    val states = statesAndCities.keys.toSeq
    val randomStateIndex = Random.nextInt(states.length) // Selecciona un índice aleatorio
    val randomState = states(randomStateIndex)
    val cities = statesAndCities(randomState)
    val randomCityIndex = Random.nextInt(cities.length) // Selecciona un índice aleatorio para la ciudad
    val randomCity = cities(randomCityIndex)
    (randomState, randomCity)
  }

  def generateAqiValueAndColor(): (Double, String) = {
    // Genera un valor AQI ponderado para que sea más difícil obtener valores altos
    val aqiValue = Random.nextInt(1000) match {
      case v if v < 500 => Random.nextInt(51) // Green
      case v if v < 750 => 51 + Random.nextInt(50) // Yellow
      case v if v < 900 => 101 + Random.nextInt(50) // Orange
      case v if v < 970 => 151 + Random.nextInt(50) // Red
      case v if v < 994 => 201 + Random.nextInt(100) // Purple
      case _ => 301 + Random.nextInt(200) // Maroon
    }

    // Asigna un color basándose en el valor AQI
    val aqiColor = aqiValue match {
      case v if v <= 50 => green
      case v if v <= 100 => yellow
      case v if v <= 150 => orange
      case v if v <= 200 => red
      case v if v <= 300 => purple
      case _ => maroon
    }

    (aqiValue, aqiColor)
  }

  def generateRandomTime(): String = {
    val hours = Random.nextInt(24) // genera un número entre 0 y 23 para las horas
    val minutes = Random.nextInt(60) // genera un número entre 0 y 59 para los minutos
    val seconds = Random.nextInt(60) // genera un número entre 0 y 59 para los segundos
    f"$hours%02d:$minutes%02d:$seconds%02d" // formatea los números para cumplir con el formato de dos dígitos
  }

  def generateRandomDate(startYear: Int, endYear: Int): String = {
    val startDate = LocalDate.of(startYear, 1, 1) // Fecha de inicio: 1 de enero del año inicial
    val endDate = LocalDate.of(endYear, 12, 31) // Fecha de fin: 31 de diciembre del año final
    val start = startDate.toEpochDay()
    val end = endDate.toEpochDay()
    val randomDay = start + Random.nextLong(end - start + 1) // Selecciona un día aleatorio dentro del rango
    val randomDate = LocalDate.ofEpochDay(randomDay)
    randomDate.toString // Formato YYYY-MM-DD
  }

}
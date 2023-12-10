package eci.edu.co

import java.util.Properties
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object PollutionAnalysis extends SparkSessionWrapper{

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    avgYearMoth
    avgState
    countColorbyStateCity
    comparationPrevYears
    maxAqi_valueStateCity
    stateCitySeason
    correlationMonthAqi_value
    stddev_aqiByStateCity
    forecast2023
  }


  val jdbcUrl = "jdbc:postgresql://localhost:5438/postgres"
  val connectionProperties = new Properties()
  connectionProperties.put("user", "postgres")
  connectionProperties.put("password", "postgres")


  val jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5438/postgres")
    .option("dbtable", "public.pollution_data")
    .option("user", "postgres")
    .option("password", "postgres")
    .load()


  //IMPLEMENTADOS

  //Promedio de aqi_value por ciudad, año y mes
  def avgYearMoth = {
    val dfYearMoth = jdbcDF
      .withColumn("month", month(col("date")))
      .withColumn("year", year(col("date")))
      .groupBy("state", "city", "year", "month")
      .agg(avg("aqi_value").alias("avg_aqi"))
      .withColumn("aqi_color",
        when($"avg_aqi" <= 50, "green")
          .when($"avg_aqi" <= 100, "yellow")
          .when($"avg_aqi" <= 150, "orange")
          .when($"avg_aqi" <= 200, "red")
          .when($"avg_aqi" <= 300, "purple")
          .otherwise("maroon")
      )
      .withColumn("month_t",
        when($"month" === 1, "January")
          .when($"month" === 2, "February")
          .when($"month" === 3, "March")
          .when($"month" === 4, "April")
          .when($"month" === 5, "May")
          .when($"month" === 6, "June")
          .when($"month" === 7, "July")
          .when($"month" === 8, "August")
          .when($"month" === 9, "September")
          .when($"month" === 10, "October")
          .when($"month" === 11, "November")
          .when($"month" === 12, "December")
      )
      .orderBy("state", "city", "year", "month")
      .select("state","city", "month_t", "year", "avg_aqi", "aqi_color")

      dfYearMoth.write.mode("overwrite").jdbc(jdbcUrl, "avgYearMoth", connectionProperties)
      dfYearMoth.show()
  }

  //Promedio por estado a lo largo del 2019-2022.
  def avgState = {
    val stateAverageAQI = jdbcDF
      .groupBy("state")
      .agg(avg("aqi_value").alias("avg_aqi"))
      .withColumn("aqi_color",
        when($"avg_aqi" <= 50, "green")
          .when($"avg_aqi" <= 100, "yellow")
          .when($"avg_aqi" <= 150, "orange")
          .when($"avg_aqi" <= 200, "red")
          .when($"avg_aqi" <= 300, "purple")
          .otherwise("maroon")
      )
      .orderBy(desc("avg_aqi"))

    stateAverageAQI.write.mode("overwrite").jdbc(jdbcUrl, "avgState", connectionProperties)
    stateAverageAQI.show()
  }

  //Conteo de veces que se ha llegado a los colores "red", "purple" y "maroon" por ciudad.
  def countColorbyStateCity = {
    val colorCounts = jdbcDF
      .filter($"aqi_color".isin("red", "purple", "maroon"))
      .groupBy("state", "city", "aqi_color")
      .count()
      .orderBy("state", "city", "aqi_color")

    colorCounts.write.mode("overwrite").jdbc(jdbcUrl, "countColorbyStateCity", connectionProperties)
    colorCounts.show()
  }

  //Muestra el promedio por ciudad, estado y año de aqi_value comparandolo con el año anterior para determinar si incrementó.
  def comparationPrevYears = {//Tabla lista
    val dfYearMoth = jdbcDF
      .withColumn("year", year($"date"))
      .groupBy("year", "state", "city")
      .agg(avg("aqi_value").alias("average_aqi"))

    val windowSpec = Window.partitionBy("state", "city").orderBy("year")
    val yearlyAvgWithLag = dfYearMoth.withColumn("prev_year_avg", lag($"average_aqi", 1).over(windowSpec))

    val compareWithPrevYear = yearlyAvgWithLag.withColumn("comparison",
      when($"average_aqi" === $"prev_year_avg", "EQUAL")
        .when($"average_aqi" > $"prev_year_avg", "INCREASE")
        .when($"prev_year_avg".isNull, "NO DATA")
        .otherwise("DECREMENT")
    ).filter($"comparison" === "INCREASE")

    compareWithPrevYear.write.mode("overwrite").jdbc(jdbcUrl, "comparationPrevYears", connectionProperties)
    compareWithPrevYear.show()
  }

  //Valores máximos de aqi_value por para cada ciudad en cada estado
  def maxAqi_valueStateCity = {//Tabla lista
    val windowSpec = Window.partitionBy("state", "city")

    val maxAQIByCity = jdbcDF.withColumn("max_aqi_by_city", max("aqi_value").over(windowSpec))
    val topPollutionByCity = maxAQIByCity.filter($"aqi_value" === $"max_aqi_by_city")
      .select("date", "hour", "state", "city", "max_aqi_by_city", "aqi_color")
      .orderBy(asc("city"), asc("state"))

    topPollutionByCity.write.mode("overwrite").jdbc(jdbcUrl, "maxAqi_valueStateCity", connectionProperties)
    topPollutionByCity.show()
  }

  //El valor máximo, mínimo y el promedio que se llegó por ciudad en en las estaciones del año por ciudad.
  def stateCitySeason = {//Tabla lista
    val scs = jdbcDF
      .withColumn("month", month(col("date")))
      //.withColumn("year", year(col("date")))
      .withColumn("season",
        when(col("month").isin(12, 1, 2), "Winter")
          .when(col("month").isin(3, 4, 5), "Spring")
          .when(col("month").isin(6, 7, 8), "Summer")
          .otherwise("Autumn")
      )
      .groupBy("state", "city", "season")
      .agg(
        avg("aqi_value").alias("average_aqi"),
        max("aqi_value").alias("max_aqi"),
        min("aqi_value").alias("min_aqi"))
      .orderBy("state", "city", "season")

    scs.write.mode("overwrite").jdbc(jdbcUrl, "stateCitySeason", connectionProperties)
    scs.show()
  }

  def correlationMonthAqi_value = {
    /*
    * Cálculo de Correlación: Calcula el coeficiente de correlación de Pearson entre estas dos columnas.
    * El coeficiente de Pearson es una medida que indica el grado de relación lineal entre dos variables.
    * El valor resultante está en el rango de -1 a 1, donde:
    *   +1 indica una correlación positiva perfecta (cuando una variable aumenta, la otra también).
    *   -1 indica una correlación negativa perfecta (cuando una variable aumenta, la otra disminuye).
    *   0 indica que no hay correlación lineal entre las variables.
    */
    val correlation = jdbcDF
      .withColumn("month", month(col("date")))
      .groupBy("state", "city")
      .agg(
        corr("month", "aqi_value").alias("correlation_month-aqi_value")
      )
    correlation.write.mode("overwrite").jdbc(jdbcUrl, "correlationMonthAqi_value", connectionProperties)
    correlation.show()
  }

  def stddev_aqiByStateCity = {
    // Calculando la desviación estándar poblacional por estado y ciudad
    /*
    Valor Promedio Alto con Desviación Estándar Baja:
    Si el valor promedio de AQI es alto y la desviación estándar es baja,
    esto indica que la calidad del aire es consistentemente mala.
    Los valores de AQI están agrupados cerca del promedio, que es alto,
    lo que sugiere que hay una calidad del aire consistentemente insalubre.

    Valor Promedio Alto con Desviación Estándar Alta:
    Si el valor promedio de AQI es alto y la desviación estándar también es alta,
    esto indica que, aunque la calidad del aire es generalmente mala, hay mucha variabilidad.
    Esto podría significar que hay días con una calidad del aire extremadamente mala mezclados con días de mejor calidad del aire.

    Valor Promedio Bajo con Desviación Estándar Baja:
    Si el valor promedio de AQI es bajo y la desviación estándar es baja,
    esto sugiere que la calidad del aire es consistentemente buena y los valores de AQI están agrupados cerca del promedio bajo.

    Valor Promedio Bajo con Desviación Estándar Alta:
    Si el valor promedio de AQI es bajo pero la desviación estándar es alta,
    esto indica que la calidad del aire varía considerablemente, aunque en promedio puede ser buena.
    */
    val stddevByStateAndCity = jdbcDF
      .groupBy("state", "city")
      .agg(
        avg($"aqi_value").alias("avg_aqi"),
        stddev_pop($"aqi_value").alias("stddev_aqi")
      )
      //.filter($"avg_aqi" > 150)
      .withColumn("aqi_color",
        when($"avg_aqi" <= 50, "green")
          .when($"avg_aqi" <= 100, "yellow")
          .when($"avg_aqi" <= 150, "orange")
          .when($"avg_aqi" <= 200, "red")
          .when($"avg_aqi" <= 300, "purple")
          .otherwise("maroon")
      )
      .orderBy("state", "city", "stddev_aqi")

    stddevByStateAndCity.write.mode("overwrite").jdbc(jdbcUrl, "stddev_aqiByStateCity", connectionProperties)
    stddevByStateAndCity.show()
  }

  //Promedio movil simple:
  /*
  El promedio móvil simple (SMA, por sus siglas en inglés)
  calcula el promedio de un número seleccionado de puntos de datos durante un período específico.
  Por ejemplo, un SMA de 10 días sumaría los precios de cierre de los últimos 10 días y luego dividiría esa suma por 10.*/
  def forecast2023 = {
    val df  = jdbcDF
      .withColumn("year", year($"date"))
      .withColumn("month", month($"date"))

    val windowSize = 12 // Para un promedio móvil de 12 meses
    val windowSpec = Window.partitionBy("year", "month").orderBy("date").rowsBetween(-windowSize, 0)

    val movingAvgDF = df.withColumn("moving_avg", avg($"aqi_value").over(windowSpec))

    val latestMovingAvgDF = movingAvgDF.filter($"year" === 2022)
      .groupBy("state","city","month")
      .agg(last("moving_avg").alias("latest_moving_avg"))

    val months = (1 to 12).toList
    val forecast2023DF = months.toDF("month")
      .join(latestMovingAvgDF, Seq("month"), "left")
      .na.fill(Map("latest_moving_avg" -> 0)) // Rellena con 0 o un valor adecuado si no hay datos
      .withColumn("year", lit(2023))
      .orderBy("state","city","month")
      .filter($"latest_moving_avg" > 150)
      .withColumn("aqi_color",
        when($"latest_moving_avg" <= 50, "green")
          .when($"latest_moving_avg" <= 100, "yellow")
          .when($"latest_moving_avg" <= 150, "orange")
          .when($"latest_moving_avg" <= 200, "red")
          .when($"latest_moving_avg" <= 300, "purple")
          .otherwise("maroon")
      )
      .withColumn("month_t",
        when($"month" === 1, "January")
          .when($"month" === 2, "February")
          .when($"month" === 3, "March")
          .when($"month" === 4, "April")
          .when($"month" === 5, "May")
          .when($"month" === 6, "June")
          .when($"month" === 7, "July")
          .when($"month" === 8, "August")
          .when($"month" === 9, "September")
          .when($"month" === 10, "October")
          .when($"month" === 11, "November")
          .when($"month" === 12, "December")
      )
      .select("month_t", "state", "city", "latest_moving_avg", "aqi_color")

    forecast2023DF.write.mode("overwrite").jdbc(jdbcUrl, "forecast2023", connectionProperties)
    forecast2023DF.show()
  }

}

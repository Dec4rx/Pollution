package eci.edu.co
package adapters.postgresql

import domain.Pollution

import org.apache.commons.dbcp2.BasicDataSource

case class PollutionDAO(dataSource: BasicDataSource){
  private val insertStatement = s"INSERT INTO pollution_data (state,city,aqi_value,aqi_color,hour,date,created) VALUES(?, ?, ?, ?, ?, ?, ?)"

  def savePollution(pollution: Pollution): Either[Throwable, Int] = {
    try{
      val connection = dataSource.getConnection
      val preparedStatement = connection.prepareStatement(insertStatement)
      preparedStatement.setString(1, pollution.state)
      preparedStatement.setString(2, pollution.city)
      preparedStatement.setDouble(3, pollution.aqiValue)
      preparedStatement.setString(4, pollution.aqiColor)
      preparedStatement.setTime(5, java.sql.Time.valueOf(pollution.hour))
      preparedStatement.setDate(6, java.sql.Date.valueOf(pollution.date))
      preparedStatement.setTimestamp(7, pollution.created)
      val updateResult = preparedStatement.executeUpdate()
      connection.close()
      Right(updateResult)
    } catch {
      case e: Exception => Left(e)
    }
  }
}

object PollutionDAO{

  def getTransactionDAO: PollutionDAO = {
    val dbUrl = "jdbc:postgresql://localhost:5438/postgres"
    val connectionPool = new BasicDataSource()
    connectionPool.setUsername("postgres")
    connectionPool.setPassword("postgres")
    connectionPool.setDriverClassName("org.postgresql.Driver")
    connectionPool.setUrl(dbUrl)
    connectionPool.setInitialSize(5)
    PollutionDAO(connectionPool)
  }
}

package eci.edu.co
package adapters.postgresql

import domain.Pollution
import domain.ports.ForUpdatingRepository



object PostgreSQL extends ForUpdatingRepository{
  lazy val pollutionDAO = PollutionDAO.getTransactionDAO


  override def savePollution(pollution: Pollution): Either[Throwable, Pollution] = {
    pollutionDAO.savePollution(pollution) match {
      case Left(e) =>
        println(s"There was an error saving the Pollution: ${e.getMessage}")
        Left(e)
      case Right(_) =>
        println("Record inserted")
        Right(pollution)
    }

  }
}

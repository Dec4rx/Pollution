package eci.edu.co
package domain.ports

import domain.Pollution

trait ForUpdatingRepository {

  def savePollution(pollution: Pollution): Either[Throwable, Pollution]

}

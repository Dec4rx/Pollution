package eci.edu.co
package domain.commands

import domain.Pollution
import domain.ports.ForUpdatingRepository

case class SavePollution(forUpdatingRepository: ForUpdatingRepository,pollution: Pollution) extends Command[Pollution] {

  override def execute(): Either[Throwable, Pollution] = forUpdatingRepository.savePollution(pollution)
}

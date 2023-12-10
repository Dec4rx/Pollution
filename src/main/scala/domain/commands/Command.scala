package eci.edu.co
package domain.commands

trait Command [T] {
  def execute(): Either[Throwable, T]
}

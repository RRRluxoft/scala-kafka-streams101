package example

object Hello extends Greeting with App { self: Greeting =>
  println(greeting)
}

trait Greeting {
  lazy val greeting: String = "4ever"
}

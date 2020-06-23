package example

object Hello {

	def main(args:Array[String]) {
	    val nom = List("Conakry","Lome","Dakar")
		nom.map(elem=> elem.toUpperCase).foreach(println)
	}
}

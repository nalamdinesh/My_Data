object M2_2LazyValue extends App{
  val normalVal = {
    println("---->>>   Initializing normal val    <<<----");
    "This is the normal val"
}
 
lazy val lazyVal = {
  println("---->>>   Initializing lazy val   <<<----");
  "This is the lazy val"
}

println ("\n\nno references have been made yet\n\n")

println ("Accessing normal val : ")
println(normalVal)
println ("Second time Accessing normal val : ")
println(normalVal)

println ("\n\nAccessing lazy val : ")
println(lazyVal)
println ("\n\nAccessing lazy val a second time, there should be no initialization now: ")
println(lazyVal)

}
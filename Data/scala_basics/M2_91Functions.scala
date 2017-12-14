object M2_10Functions extends App {

  println("Total Value : " + addInt(4, 5));

  def addInt(a: Int, b: Int): Int = {
    var mysum: Int = 0
    mysum = a + b

    return mysum
  }

  println("Calling no return function" + addIntNoReturn(12, 8));


  //This is also called as procedure
  //preceding "=" symbol missing
  def addIntNoReturn(a: Int, b: Int) {
    var mysum: Int = 0
    mysum = a + b
    println("Total Value println inside function :" + mysum)

  }
}
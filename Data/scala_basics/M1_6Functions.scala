object M2Functions {
   def main(args: Array[String]) {
        println( "Total Value : " + addInt(4,5) );
   }
   def addInt( a:Int, b:Int ) : Int = {
      var mysum:Int = 0
      mysum = a + b
      return mysum
   }
}
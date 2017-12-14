object M2FunctionDefaultParam {
   def main(args: Array[String]) {
        println( "Returned Value : " + addValues() );
        println( "Returned Value : " + addValues(6) );
        println( "Returned Value : " + addValues(6,3) );
        println( "Returned Value : " + addValues(b=3) );
   
   }
   def addValues( a:Int=4, b:Int=2 ) : Int = {
      var sum:Int = 0
      sum = a + b

      return sum
   }
}
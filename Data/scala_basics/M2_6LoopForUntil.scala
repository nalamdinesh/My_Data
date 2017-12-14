object M2LoopForUntil {
   def main(args: Array[String]) {
      var a = 0;


      // for loop execution with a range
      for( a <- 1 until 8){
         println( "This is value of a (using until): " + a );
      }
   }
}
object M2LoopForMultiRange {
   def main(args: Array[String]) {
      var a = 0;
      var b = 0;
     /*
      // for loop execution with a range
      for( a <- 1 to 3; b <- 1 to 3){
         println( "This is value of a: " + a );
         println( "This is value of b: " + b );
      }
      */
      //mutlirange with condition
           for( a <- 1 to 3; b <- 1 to 3 if a==b){
         println( "This is value of a with condition: " + a +" :: "+ b);
         //println( "This is value of b with condition: " + b );
      }
      
      
   }
}
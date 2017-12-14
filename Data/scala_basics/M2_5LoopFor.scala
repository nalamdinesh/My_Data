object M2LoopFor {
   def main(args: Array[String]) {
      var a = 0;
      // for loop execution with a range
      // for( a <- 1 to 5){
   // for( a <- 5 to 1 by -1){
      // for( a <- 5 to 1 by -1){
       //for( a <- 5 to 1 by -2){
       for( a <- 1 to 5 by 2){
         
         println( "This is the value of a: " + a );
      }
       
      var array1 = Array(1.9, 2.9, 3.4, 3.5) 
      for ( x <- array1 ) {
         println( x )
      }
             
   }
}
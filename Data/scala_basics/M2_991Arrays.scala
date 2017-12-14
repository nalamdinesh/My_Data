import Array._
object M2_991Arrays extends App {

      var array1 = Array(1.9, 2.9, 3.4, 3.5)
      
      // Print all the array elements
      for ( x <- array1 ) {
         println( x )
      }
      
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Multi Dimensional Array")
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      
      //Two Dimensional Array
      //need to import Array package
      
      var arrayMultiDim = ofDim[Int](3,3)
      
      // build a matrix
      for (i <- 0 to 2) {
         for ( j <- 0 to 2) {
            arrayMultiDim(i)(j) = j;
         }
      }
      
      // Print two dimensional array
      for (i <- 0 to 2) {
         for ( j <- 0 to 2) {
            print(" " + arrayMultiDim(i)(j));
         }
         println();
      }
      
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Array Concatenation")
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
 
      
      var mycat1 = Array(1.2, 2.9, 3.2)
      var mycat2 = Array(99.9, 12.9, 34.4, 11.5)

      var finalcat =  concat( mycat1, mycat2)
      
      // Print all the array elements
      for ( x <- finalcat ) {
         println( x )
      }
      
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Array Ranges")
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")    
      
      var firstRange = range(10, 20, 2)
      var secondRange = range(10,20)
      var secondRange1 = range(1,500)

      // Print all the array elements
      for ( x <- firstRange ) {
         print( " " + x )
      }
      println()
      for ( x <- secondRange ) {
         print( " " + x )
      }
      
      
  
      
      
}
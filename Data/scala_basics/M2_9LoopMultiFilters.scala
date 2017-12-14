object M2LoopMultiFilters {
   def main(args: Array[String]) {
      var count = 0;
      val myList = List(1,2,3,4,5,6,7,8,9,10);

      // for loop execution with multiple filters
      for( count <- myList if count != 3; if count < 8 )
      {
         println( "This is value of count: " + count );
      }
   }
}
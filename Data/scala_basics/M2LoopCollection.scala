object M2LoopCollection {
   def main(args: Array[String]) {
      var count = 0;
      val myList = List(1,2,3,4,5,6,7,8);

      // for loop execution with a collection
      for( count <- myList ){
         println( "This is value of count: " + count );
      }
      
      println("foreach sample")
    		  myList.foreach(element=>println("Value from foreach "+element))
   }
}
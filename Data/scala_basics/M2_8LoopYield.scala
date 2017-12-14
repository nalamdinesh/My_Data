object M2LoopYield {
   def main(args: Array[String]) {
      var count = 0;
      val myList = List(1,2,3,4,5,6,7,8,9,10);

      // for loop execution with a yield
     
      var yieldedVal = for{ count <- myList 
                        if count != 3; if count < 8
                      }yield count

                    
                   
          println(yieldedVal)
          println(yieldedVal.head)
          println(yieldedVal(2))
          
 
      // Now print returned values using another loop.
      for( myval <- yieldedVal){
         println( "This is value of myval: " + myval );
      }
   }
}
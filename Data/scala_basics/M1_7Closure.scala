

//A closure is a function, whose return value depends on 
//the value of one or more variables declared outside this function

object closure {
   def main(args: Array[String]) {
      println( "muliplier(1) value = " +  multiplier(1) )
      println( "muliplier(2) value = " +  multiplier(2) )
   }
   var factor = 3
   
   // anonymous function
   val multiplier = (i:Int) => i * factor
   
}
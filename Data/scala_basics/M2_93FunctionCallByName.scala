object M2FunctionCallByName {
   def main(args: Array[String]) {
        delayed(time());

   }

   def time() = {
      println("Current time in nano seconds")
      System.nanoTime
   }
    
   
   //parameter as value
  def delayed( time: Long ) = {
   //parameter as expression
   //def delayed( time: => Long ) = {
      println("In delayed method")
      println("Param: " + time)
      println("Param: " + time)
   }
  
}
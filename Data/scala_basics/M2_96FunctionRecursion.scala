object M2FunctionRecursion {
   def main(args: Array[String]) {
      for (i <- 1 to 10)
         println( "Factorial value of " + i + ": = " + factorial(i) )
   }
   
   def factorial(count: BigInt): BigInt = {  
      if (count <= 1)
         1  
      else    
     count * factorial(count - 1)
   }
}
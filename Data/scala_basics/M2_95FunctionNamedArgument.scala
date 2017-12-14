object M2FunctionNamedArgument {
   def main(args: Array[String]) {
       // printInt(b=4, a=3);
        printInt1(5,6,d=44, c=33);

        
        println("With Variable arguments with different param location")
        
        println("==============")
        printInt(b=4, a=3);
        println("==============")
        printInt(4,3)
       
        println("With Variable arguments")
        printInt3(b=6,c=33);
        
        println("With Variable arguments and a default value")
        printInt1(6,d=44, c=33);

        
       
   }
   def printInt( a:Int, b:Int ) = {
    // def printInt( b:Int, a:Int ) = {
      println("This is value of a : " + a );
      println("This is value of b : " + b );
   }
   
     def printInt3( a:Int=0, b:Int=0, c:Int=0 ) = {
    // def printInt( b:Int, a:Int ) = {
      println("This is value of a : " + a );
      println("This is value of b : " + b );
   }
   
    def printInt1( a:Int=0, b:Int=0, c:Int=0, d:Int=0 ) = {
    // def printInt( b:Int, a:Int ) = {
      println("This is value of a : " + a );
      println("This is value of b : " + b );
       println("This is value of c : " + c );
        println("This is value of d : " + d );
   }
   
}
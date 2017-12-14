object M2FunctionVariableArgument {
   def main(args: Array[String]) {
        printStrings("Welcome", "To", "Spark", "Training");
        println("===================")
        printStrings("Welcome", "To", "Spark");
        
   }
   
   //similar to java variable length argument
   def printStrings( args:String* ) = {
      var count : Int = 0;
      for( arg <- args ){
         println("Arg value[" + count + "] = " + arg );
         count = count + 1;
      }
   }
}
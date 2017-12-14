object M2_92FunctionNoReturn extends App {

      
  //function which doesnt pass any value or retun any value
  val f : () => Unit = () => println("Hello this is ()=>Unit")
    
    //invoking the function
    f()
    
    val f1 = () => println("""Since Scala is type inference you 
        dont have to declare the type 
        This would also work as above""")
    
    f1()
    
    //since function is object below statement also holds good
    f1.apply()

}
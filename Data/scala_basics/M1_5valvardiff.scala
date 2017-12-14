

object m1valvardiff {

  def main(args: Array[String]) {

    var myVar: String = "this is var"
    println("Printing intial assigned value :: "+myVar)

    myVar = "changed"
    println("Printing changed value :: "+myVar)


    
    //Type Inference
    //myVar = 3
    
    //var myInt = "kumar"
    
    var myInt = 3
    println(myInt.getClass)
    
    //immutable
    val myVal: String = "this is val"
    println(myVal)
    
    
   // myVal = "try changing"
    
    
    /*
     
//mutable
var myVar : String = "this is var"
println(myVar)



//immutable
val myVal : String = "this is val"
println(myVal)

myVal = "try changing"



Valid 

var myVar :Int; 
val myVal :String;

//automatically scala compiler will figure out the type as int
var myVar = 10; 

//automatically scala compiler will figure out the type as String
val myVal = "Hello, Scala!";
*/
     
  }
}
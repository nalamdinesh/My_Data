import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

def main(args: Array[String]){

  val a = ArrayBuffer[Int]()
  a +=1
  
  for(el<-a) println(el)
  
  println("Adding (2,3,4)")
  a +=(2,3,4)

  for(el<-a) println(el)
  
  println("Adding Array(6,7,8)")
  a ++=Array(6,7,8)
  //a.++(Array(6,7,8))
  for(el<-a) println(el)
  
    
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    println("Transformation")
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  

  //Transformation
  var yieldedVal = for (el <- a if el % 2==0) yield (2*el)
  
  println(yieldedVal)
  
  println("Yielded value "+yieldedVal.mkString(" ** "))
  println("Yielded value "+yieldedVal.mkString)
  
  	println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    println("Sorting")
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  
  
    val mySortTest = Array(1,7,2,9)
    println("Actual value "+mySortTest.mkString(" ** "))
    scala.util.Sorting.quickSort(mySortTest)
    println("Sorted value "+mySortTest.mkString(" ** "))
    


  
}
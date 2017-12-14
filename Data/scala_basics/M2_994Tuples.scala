object M2_994Tuples extends App {

   val t = (11,22,33,44)

   val sum = t._1 + t._2 + t._3 + t._4
   println("First Element : " + t._1)

   println( "Sum of elements: "  + sum )
   
   //Start with 1 and not from 0
   println( "First Value: "  + t._1 )
   
  
   
   
   val intList = List(2,7,9,1,6,5,8,2,4,6,2,9,8)
   val (big,small) = intList.partition (_ > 5)
   val quicktry = intList.partition (_ > 2)
   
   println(" big numbers" + big)
   println(" small numbers" + small)
   println(" quicktry"+quicktry._1)
   println(" quicktry"+quicktry._2)
    println(" quicktry"+quicktry)
    println(" quicktry"+quicktry.getClass)
   
   
}
object M2_993Maps extends App{
  
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    println("Maps")
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")  
    
    val mapping1 = Map("Vishal"->"Kumar","Vijay"->"Verma","Vijay"->"Verma1")
    
    //Default Map will be immutable. For Mutable explicit import is required
   // mapping1 += ("Muthu" -> "Kumar")
    
    val mapping2 = scala.collection.mutable.Map("Vishal"->"K","Vijay"->"K")
    
   // mapping2 = scala.collection.mutable.Map("AnotherVishal"->"K","AnotherVijay"->"K")
    
    
    //mapping1 += ("key3" -> "Kumar")
   mapping2 += ("key3" -> "Kumar")
     
   



    println( "printing keys of mapping1: " + mapping1.keys )
    println( "printing values of  mapping1: " + mapping1.values )
    
    println( "printing keys of mapping2: " + mapping2.keys )
    println( "printing values of  mapping2: " + mapping2.values )
    
    println( "getting a value of Vishal from mapping2 : " + mapping2.getOrElse("Vishal",0) )
        println( "getting a value of Viss from mapping2: " + mapping2.getOrElse("Viss","Not Found") )

    
    
    //try the following in Interpreter. Return type will be Any
    
    //val mapping2 = scala.collection.mutable.Map("Vishal"->"K","Vijay"->"V")
    //mapping2.getOrElse("Vish",0)
    
    val mapping3 = for((k,v)<-mapping2) yield (v,k)
    
        println(mapping2)
        println(mapping3)
        println( "printing keys of mapping3: " + mapping3.keys )
        println( "printing keys of mapping3: " + mapping3 )

    
}
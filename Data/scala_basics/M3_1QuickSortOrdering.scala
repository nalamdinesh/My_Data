import scala.util.Sorting
object M3_1QuickSortOrdering extends App {

    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    println("Multi Dimensional Array Sorting")
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~") 
    
    val pairs = Array(("a", 5, 2), ("c", 1, 1), ("b", 3, 1))
    
    Sorting.quickSort(pairs)(Ordering.by[(String, Int, Int), Int](_._2))
    
    println("Sorting based on 2nd Element "+pairs.mkString(" ** "))
    
    Sorting.quickSort(pairs)(Ordering[(Int, String)].on(x => (x._3, x._1)))
    
    println("Sorting by third element and then first element "+pairs.mkString(" ** "))
}
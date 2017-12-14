object M21AssigningBlock extends App {

      var myBlock = {val a=10;val b=20;b-a}
            
    println("Printing myBlock :: "+myBlock)
    
    var myBlock1 = {val a=10;val b=20;b-a;b+a}
    println("Printing myBlock1 :: "+myBlock1)
    
    var myBlock2 = {val a=10;val b=20}
    println("Printing myBlock1 :: "+myBlock2)
    
        var myBlock3 = {val a=10;val b=20;println(a+b);b}
    println("Printing myBlock1 :: "+myBlock3)
    
}
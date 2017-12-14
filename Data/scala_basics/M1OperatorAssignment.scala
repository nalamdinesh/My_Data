object M1OperatorAssignment {
	def main(args: Array[String]) { 
	  var a = 20; 
	  var b = 40; 
	  var c = 0; 
	  c = a + b; 
	  
	  
	  println("c = a + b = " + c ); 
	  
	  c += a ; 
	  println("c += a = " + c );
	 
	  c -= a ; 
	  println("c -= a = " + c ); 
	  
	  c *= a ; 
	  println("c *= a = " + c );
	  
	  c = 15; c /= a ; 
	  println("c /= a = " + c ); 
	  
	  a = 10; 
	  c = 15; 
	  c %= a ; 
	  println("c %= a = " + c ); 
	  
	  c <<= 2 ;
	  println("c <<= 2 = " + c ); 
	  
	  c >>= 2 ; 
	  println("c >>= 2 = " + c ); 
	  
	  c >>= 2 ; 
	  println("c >>= a = " + c ); 
	  
	  c &= a ; 
	  println("c &= 2 = " + c ); 
	  
	  c ^= a ; 
	  println("c ^= a = " + c );
	  
	  
	  c |= a ; 
	  println("c |= a = " + c );
	  
	}
}
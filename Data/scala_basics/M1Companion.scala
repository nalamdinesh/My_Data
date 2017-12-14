// An object with the same name as a class or trait and is defined in the same source file as the associated file or trait
//Will discuss in Module 3

//uses - define factory methods for class
// Companion class and object cannot be executed in the interpreter


class M1ClassCompanion {
    def sayHelloWorld() {
        println("Hello from class124");
    }
}


object M1ObjectCompanion {
   def sayHi() {
        println("Hello from object");
    }
}

//Below command to be executed from interpretor

//var classCompanion = new M1ClassCompanion()
//classCompanion.sayHelloWorld()
//M1ObjectCompanion.sayHi()

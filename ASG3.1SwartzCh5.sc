/**ASG3.1SwartzCDh5
 *
 * 2022-08-29
 */
  //Q 1 find max of a,b, write hof for a,b,c
type I = Int; type D = Double; type S= String
type B = Boolean
//scala II ( not yet scala scala III )
def max (a: I, b: I):I =  if (a >=  b) a else b
def hofmax( a:I, b: I , c: I, f: (I,I) => I): I={
  val tempmax = if (a >= b) a else b
  f(tempmax,c)
}
hofmax(2,7, -12, max)
def min(a:I, b:I):I = if(a<=b) a else b
def randy :I = scala.util.Random.nextInt()
val x = randy
val y = randy
max(x,y)
/* Q3   write a hof that takes an I and returns a fun
the returned fun should take an arg, "x"
*/
def fun1 ( a: I): I =>I ={
   x:I => x * a
}
val f = fun1(3)   // this returns a function taking "x"
          // this function already has "a" in it
f(10)
/* Q6 write function "conditional"that takes
a value x, ( of arbitrary type, say Double)) and two
functions, p ,f  where  p returns boolean.
f takes x and returns some value of type Double
invoke f only if p returns true
the function conditional returns a value of type Double
overall
 */
def conditional(x: D, p: D => B, f: D => D): D ={
  if (p(x)) f(x)  else x
}
conditional(12.0, x => x<12, x => x * x)
conditional(12.0, x => x>=12, x => x* x)







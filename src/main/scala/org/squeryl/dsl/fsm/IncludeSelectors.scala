package org.squeryl.dsl.fsm

import org.squeryl.dsl.OneToMany

trait IncludePath[M] {
  def member[P](i: M => OneToMany[P]): IncludePath[P] = new OneToManyIncludePath[M, P](i)
}

object Path {
  implicit class ObjectToPath[R](r: R) extends IncludePath[R]
}

class OneToManyIncludePath[M, P](accessor: M => OneToMany[P]) extends IncludePath[P]

//
//import org.squeryl.Schema
//import org.squeryl.dsl.{OneToMany, QueryYield}
//
//trait IncludeSelector[R, P, R1, P1] {
//  def child[G](s: IncludeFactory[P, G, R, P] => OneToManyIncludeChildSelector[P, G, R, P]): OneToManyIncludeChildSelector[P, G, R, P]
//  def inSelected: QueryYield[R1]
//}
//
//trait IncludeFactory[R, P, R1, P1] {
//  def withField[A](p: R => OneToMany[A]): OneToManyIncludeChildSelector[R, A, R1, P1]
//}
//
//class OneToManyIncludeSelector[R, P]
//  (val selector: R => OneToMany[P],
//   val includeYield: QueryYield[R])
//  (implicit s: Schema)
//  extends IncludeSelector[R, P, R, P]
//{
//  def child[G](s: IncludeFactory[P, G, R, P] => OneToManyIncludeChildSelector[P, G, R, P]): OneToManyIncludeChildSelector[P, G, R, P] = new OneToManyIncludeChildSelector[P, G, R, P](s, this)
//  def inSelected: QueryYield[R] = includeYield
//}
//
//class OneToManyIncludeChildSelector[R, P, R1, P1](selector: R => OneToMany[P], root: OneToManyIncludeSelector[R1, P1] )
//  extends IncludeSelector[R, P, R1, P1](selector) {
//
//  override def child[G](s: (P) => OneToMany[G]): OneToManyIncludeChildSelector[P, G, R1, P1] = new OneToManyIncludeChildSelector[P, G, R1, P1](s, root)
//
//  def inSelected: QueryYield[R1] = ???
//}

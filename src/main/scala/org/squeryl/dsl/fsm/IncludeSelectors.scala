package org.squeryl.dsl.fsm

import org.squeryl.KeyedEntity
import org.squeryl.dsl.OneToMany

trait IncludePath[M] extends IncludePathBase {

  def member[P](i: M => OneToMany[P]): IncludePath[P] = new OneToManyIncludePath[M, P](i)
//  def members(i: (M => OneToMany[Any])*): IncludePath[Any] = new OneToManyIncludePath[M, Any](i.head)
//  def member[P](i: M => IncludePath[P]): IncludePath[P] = ???
}

trait IncludePathBase {
  def invoke[T](o: Any): T
}

object Path {
  implicit class ObjectToPath[M](r: M) {
    def member[P](i: M => OneToMany[P]): IncludePath[P] = new OneToManyIncludePath[M, P](i)
    def members(i: (M => IncludePathBase)*): IncludePathBase = new OneToManyIncludePath[M, Any](null)
  }

  implicit class OneToManyToPath[M](r: OneToMany[M]) {
    def member[P](i: M => OneToMany[P]): IncludePath[P] = new OneToManyIncludePath[M, P](i)
//    def member[P](i: M => IncludePath[P]): IncludePath[P] = new OneToManyIncludePath[M, P](null)
//    def members(i: (M => OneToMany[_])*): IncludePath[_] = new OneToManyIncludePath[M, Any](null)
    def members(i: (M => IncludePathBase)*): IncludePathBase = new OneToManyIncludePath[M, Any](null)
  }

//  implicit class OneToManyToIncludePath[M](r: M) {
//
//  }

//  implicit def OneToManyAccessorToIncludePath[P](i: _ => OneToMany[P]): IncludePath[P] = new OneToManyIncludePath[_, P](i)
}

class OneToManyIncludePath[M, P](accessor: M => OneToMany[P]) extends IncludePath[P] {
  def invoke[T](o: Any): T = accessor(o.asInstanceOf[M]).asInstanceOf[T]
}

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

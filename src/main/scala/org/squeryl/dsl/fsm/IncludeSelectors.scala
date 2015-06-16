package org.squeryl.dsl.fsm

import org.squeryl._
import org.squeryl.dsl.OneToMany
import org.squeryl.dsl.ast.EqualityExpression
import org.squeryl.dsl.internal.{OuterJoinedQueryable, JoinedQueryable}
import org.squeryl.internals.FieldReferenceLinker

import scala.reflect.ClassTag

trait IncludePath[M] extends IncludePathBase {

  def member[P](i: M => OneToMany[P])(implicit s: Schema, mClass: ClassTag[M], pClass: ClassTag[P]): IncludePath[P] = new OneToManyIncludePath[M, P](i)
//  def members(i: (M => OneToMany[Any])*): IncludePath[Any] = new OneToManyIncludePath[M, Any](i.head)
//  def member[P](i: M => IncludePath[P]): IncludePath[P] = ???
}

trait IncludePathBase {
  def relationshipAccessor[T](o: Any): T
  def equalityExpressionAccessor(o: Any, m: Any): EqualityExpression
  def joinedQueryable: JoinedQueryable[_]
  def leftType: ClassTag[_]
  def rightType: ClassTag[_]

  protected def tableFor[A]()(implicit s: Schema, aClass: ClassTag[A]): Table[A] = {
    s.findAllTablesFor(aClass.runtimeClass).head.asInstanceOf[Table[A]]
  }

  protected def sampleFor[A](a: Table[A])(implicit s: Schema): A = {
    val v = a.asInstanceOf[View[A]]
    val vxn = v.viewExpressionNode
    vxn.sample =
      v.posoMetaData.createSample(FieldReferenceLinker.createCallBack(vxn))

    vxn.sample
  }
}

object Path {
  implicit class ObjectToPath[M](r: M) {
    def member[P](i: M => OneToMany[P])(implicit s: Schema, mClass: ClassTag[M], pClass: ClassTag[P]): IncludePath[P] = new OneToManyIncludePath[M, P](i)
    def members(i: (M => IncludePathBase)*): IncludePathBase = ??? //new OneToManyIncludePath[M, Any](null)
  }

  implicit class OneToManyToPath[M](r: OneToMany[M]) {
    def member[P](i: M => OneToMany[P])(implicit s: Schema, mClass: ClassTag[M], pClass: ClassTag[P]): IncludePath[P] = new OneToManyIncludePath[M, P](i)
//    def member[P](i: M => IncludePath[P]): IncludePath[P] = new OneToManyIncludePath[M, P](null)
//    def members(i: (M => OneToMany[_])*): IncludePath[_] = new OneToManyIncludePath[M, Any](null)
    def members(i: (M => IncludePathBase)*): IncludePathBase = ??? //new OneToManyIncludePath[M, Any](null)
  }

//  implicit class OneToManyToIncludePath[M](r: M) {
//
//  }

//  implicit def OneToManyAccessorToIncludePath[P](i: _ => OneToMany[P]): IncludePath[P] = new OneToManyIncludePath[_, P](i)
}

class OneToManyIncludePath[M, P](accessor: M => OneToMany[P])(implicit s: Schema, mClass: ClassTag[M], pClass: ClassTag[P]) extends IncludePath[P] {
  val pTable = s.findAllTablesFor(pClass.runtimeClass).head
  
  def relationshipAccessor[T](o: Any): T = accessor(o.asInstanceOf[M]).asInstanceOf[T]

  def equalityExpressionAccessor(o: Any, m: Any): EqualityExpression =
    s.findRelationsFor(mClass.runtimeClass.asInstanceOf[Class[M]], pClass.runtimeClass.asInstanceOf[Class[Any]]).head.equalityExpression.apply(o.asInstanceOf[M], m.asInstanceOf[Option[Any]].get)

  val joinedQueryable: JoinedQueryable[_] = new OuterJoinedQueryable[Any](pTable.asInstanceOf[Queryable[Any]], "left")

  def leftType: ClassTag[_] = mClass

  def rightType: ClassTag[_] = pClass
}

class EnclosingIncludePath[M](accessor: M => IncludePathBase)(implicit s: Schema, mClass: ClassTag[M]) extends IncludePathBase {
  private val table = tableFor[M]()
  private val sample = sampleFor[M](table)
  private val includePath = accessor(sample)

  def relationshipAccessor[T](o: Any): T = includePath.relationshipAccessor(o)

  def equalityExpressionAccessor(o: Any, m: Any): EqualityExpression = includePath.equalityExpressionAccessor(o, m)

  def leftType: ClassTag[_] = mClass

  def rightType: ClassTag[_] = includePath.rightType

  def joinedQueryable: JoinedQueryable[_] = includePath.joinedQueryable
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

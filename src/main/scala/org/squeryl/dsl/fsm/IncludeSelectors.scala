package org.squeryl.dsl.fsm

import org.squeryl._
import org.squeryl.dsl.OneToMany
import org.squeryl.dsl.ast.EqualityExpression
import org.squeryl.dsl.internal.{OuterJoinedQueryable, JoinedQueryable}
import org.squeryl.internals.FieldReferenceLinker

import scala.reflect.ClassTag

//trait IncludePath[M] extends IncludePathRelation {
//
//  def member[P](i: M => OneToMany[P])(implicit s: Schema, mClass: ClassTag[M], pClass: ClassTag[P]): IncludePath[P] = new OneToManyIncludePathRelation[M, P](i, new IncludePathNode[M](null), new IncludePathNode[P](null))
////  def members(i: (M => OneToMany[Any])*): IncludePath[Any] = new OneToManyIncludePath[M, Any](i.head)
////  def member[P](i: M => IncludePath[P]): IncludePath[P] = ???
//}

trait IncludePathRelation {
  def relationshipAccessor[T](o: Any): T
  def right: IncludePathCommon
}

trait IncludePathCommon {
  def classType: ClassTag[_]
  def relations: Seq[IncludePathRelation]
  def table: Table[_]
  def schema: Schema

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

trait IncludePathSelector[M] {
  def to[P](i: M => OneToMany[P])(implicit s: Schema, mClass: ClassTag[M], pClass: ClassTag[P]): IncludePathRelation  = {
    val rightNode = new IncludePathNode[P](null)
    new OneToManyIncludePathRelation[M, P](i, rightNode)
  }

  def members(i: (IncludePathSelector[M] => IncludePathRelation)*)(implicit schema: Schema, mClass: ClassTag[M]): IncludePathCommon =
    new IncludePathNode[M](i.map(_.apply(new IncludePathSelector[M] {})))
}

object Path {
  implicit class ObjectToPath[M](r: M) extends IncludePathSelector[M]

//  implicit class OneToManyToPath[M](r: OneToMany[M]) {
//    def member[P](i: M => OneToMany[P])(implicit s: Schema, mClass: ClassTag[M], pClass: ClassTag[P]): IncludePath[P] = new OneToManyIncludePathRelation[M, P](i, new IncludePathNode[M](null), new IncludePathNode[P](null))
////    def member[P](i: M => IncludePath[P]): IncludePath[P] = new OneToManyIncludePath[M, P](null)
////    def members(i: (M => OneToMany[_])*): IncludePath[_] = new OneToManyIncludePath[M, Any](null)
//    def members(i: (M => IncludePathRelation)*): IncludePathRelation = ??? //new OneToManyIncludePath[M, Any](null)
//  }

//  implicit def OneToManyToIncludePath[P](otm: OneToMany[P]): IncludePathBase = {
//
//  }

//  implicit class OneToManyToIncludePath[M](r: M) {
//
//  }

  implicit def OneToManyAccessorToIncludePath[P, M](i: P => OneToMany[M]): IncludePathRelation = ???
}

class OneToManyIncludePathRelation[M, P](accessor: M => OneToMany[P], override val right: IncludePathCommon)(implicit schema: Schema, mClass: ClassTag[M], pClass: ClassTag[P]) extends IncludePathRelation {
  val table = schema.findAllTablesFor(pClass.runtimeClass).head
  
  def relationshipAccessor[T](o: Any): T = accessor(o.asInstanceOf[M]).asInstanceOf[T]

  def equalityExpressionAccessor(o: Any, m: Any): EqualityExpression =
    schema.findRelationsFor(mClass.runtimeClass.asInstanceOf[Class[M]], pClass.runtimeClass.asInstanceOf[Class[Any]]).head.equalityExpression.apply(o.asInstanceOf[M], m.asInstanceOf[Option[Any]].get)

  val joinedQueryable: JoinedQueryable[_] = new OuterJoinedQueryable[Any](table.asInstanceOf[Queryable[Any]], "left")

  def classType: ClassTag[_] = pClass

  def relations: Seq[IncludePathRelation] = Seq()
}

class IncludePathNode[M](override val relations: Seq[IncludePathRelation])(implicit override val schema: Schema, mClass: ClassTag[M])
  extends IncludePathCommon {
  val table = tableFor[M]()
  private val sample = sampleFor[M](table)

  def classType: ClassTag[_] = mClass
}

//class AdjacentIncludePath[M](includePaths: Seq[IncludePathRelation])(implicit override val schema: Schema, mClass: ClassTag[M]) extends IncludePathRelation {
//
//  def relationshipAccessor[T](o: Any): T = ???
//
//  def classType: ClassTag[_] = mClass
//
//  def equalityExpressionAccessor(o: Any, m: Any): EqualityExpression = ???
//
//  def joinedQueryable: JoinedQueryable[_] = ???
//
//  override def children: Seq[IncludePathRelation] = includePaths
//
//  val table: Table[_] = tableFor[M]()
//}

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

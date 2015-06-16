/*******************************************************************************
 * Copyright 2010 Maxime Lévesque
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************** */
package org.squeryl.dsl.fsm

import java.lang.reflect.Method

import net.sf.cglib.proxy.{Enhancer, MethodProxy, MethodInterceptor, Callback}
import org.squeryl.dsl.ast._
import org.squeryl.dsl._
import org.squeryl.dsl.boilerplate._
import org.squeryl.dsl.internal.{JoinedQueryable, OuterJoinedQueryable}
import org.squeryl.internals.{FieldReferenceLinker, ResultSetMapper, ColumnToTupleMapper, OutMapper}
import java.sql.ResultSet

import org.squeryl._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.{Success, Try}

class BaseQueryYield[G]
  (val queryElementzz: QueryElements[_], val selectClosure: ()=>G)
  extends SelectState[G]
    with OrderBySignatures[G]
    with QueryYield[G] {

  protected def _createColumnToTupleMapper(origin: QueryExpressionNode[_], agregateArgs: List[TypedExpression[_,_]], offsetInResultSet:Int, isForGroup:Boolean) = {

    var i = -1;
    val nodes = agregateArgs.map(e => { i += 1; new TupleSelectElement(origin, e, i, isForGroup)})

    var o = offsetInResultSet

    val mappers = new Array[OutMapper[_]](agregateArgs.size)

    var k:Int = 0
    agregateArgs.foreach(e => {
      e.mapper.index = o
      o += 1;
      mappers(k) = e.mapper
      k += 1
    })

    val m = new ColumnToTupleMapper(mappers)

    for(n <- nodes)
      n.columnToTupleMapper = Some(m)
    (m, nodes)
  }

  protected var _havingClause: Option[()=>LogicalBoolean] = None

  def unevaluatedHavingClause = _havingClause

  //TODO: an array is probably more efficient, even if less 'lazy' :
  protected var _orderByExpressions: () => List[()=>ExpressionNode] = null

  def whereClause: Option[ExpressionNode] =
    queryElementzz.whereClause.map(b=>b())

  def havingClause: Option[ExpressionNode] =
    _havingClause.map(c=>c())

  def groupByClause: Iterable[ExpressionNode] = Iterable.empty

  def commonTableExpressions: Iterable[Query[_]] = queryElementzz.commonTableExpressions

  def queryElements =
    (whereClause, havingClause, groupByClause, orderByClause, commonTableExpressions)

  def computeClause:List[ExpressionNode] = List.empty

  def orderByClause: Iterable[ExpressionNode] = {
    if(_orderByExpressions == null)
      List.empty
    else
      _orderByExpressions().map(c=>c())
  }

  def invokeYield(rsm: ResultSetMapper, rs: ResultSet): G =
    selectClosure()

  def invokeYieldForAst(q: QueryExpressionNode[_], rsm: ResultSetMapper) =
    FieldReferenceLinker.determineColumnsUtilizedInYeldInvocation(
      q, rsm, ()=>invokeYield(rsm, null).asInstanceOf[AnyRef])


  protected def _sTuple1ToValue[B](b: B) =
    b match {
        case t:STuple1[_] =>
          if(t.productArity == 1)
            t._1.asInstanceOf[B]
          else b
      }

  def include(inclusion: G => IncludePathBase)(implicit s: Schema/*, gClass: ClassTag[G], pClass: ClassTag[_]*/) = {
//    val pTable = s.findAllTablesFor(pClass.runtimeClass).head.asInstanceOf[Table[_]]

    val includeExpressions = this.includeExpressions /*++ Seq(
      (Seq((new OuterJoinedQueryable[Any](pTable.asInstanceOf[Queryable[Any]], "left"),
        (r: Any, p: Any) => s.findRelationsFor(gClass.runtimeClass.asInstanceOf[Class[G]], pClass.runtimeClass.asInstanceOf[Class[Any]]).head.equalityExpression.apply(r.asInstanceOf[G], p.asInstanceOf[Option[Any]].get))),
        inclusion.asInstanceOf[Any => IncludePath[Any]]))*/

    new IncludedPropertiesQueryYield[G](this.queryElementzz, this.selectClosure, includeExpressions)
  }

  private def sampleFor[A](a: Table[A])(implicit s: Schema): A = {
    val v = a.asInstanceOf[View[A]]
    val vxn = v.viewExpressionNode
    vxn.sample =
      v.posoMetaData.createSample(FieldReferenceLinker.createCallBack(vxn))

    vxn.sample
  }
  
//  private val enhancer = new Enhancer
//    def includeDescendants[P](inclusion: (G) => Iterable[OneToMany[P]])(implicit s: Schema, rClass: ClassTag[G], pClass: ClassTag[P]): IncludedPropertiesQueryYield[G] = {
//      val gTable = s.findAllTablesFor(rClass.runtimeClass).head.asInstanceOf[Table[G]]
//      val sample = sampleFor(gTable)
//      inclusion(sample._1)
//
//      ???
//    }

//    var callBack: SampleLinkReader = null
//    private def sampleForTable[A](implicit s: Schema, aClass: Class[_]): A = {
//      val aTable = s.findAllTablesFor(aClass).head.asInstanceOf[Table[A]]
//      val sample = sampleFor(aTable)
//      sample._1
//    }
//
//    private def sampleFor[A](a: Table[A])(implicit s: Schema): (A, Callback) = {
//      val v = a.asInstanceOf[View[A]]
//      val vxn = v.viewExpressionNode
//      enhancer.setCallback(callBack)
//      vxn.sample =
//        v.posoMetaData.createSample(callBack)
//
//      (vxn.sample, callBack)
//    }
//
//    class SampleLinkReader(implicit schema: Schema) extends MethodInterceptor {
//      private val interceptedBuffer = new ArrayBuffer[Any]
//
//
//      def intercepted: Iterable[Any] = {
//        interceptedBuffer
//      }
//
//      class OneToManyProxy[M](schema: Schema, mClass: Class[_]) extends OneToMany[M] {
//        def iterator: Iterator[M] = List(sampleForTable[M](schema, mClass)).iterator
//
//        def assign(m: M): M = ???
//
//        def deleteAll: Int = ???
//
//        def associate(m: M): M = ???
//
//        private [squeryl] def fill(o: Iterable[M]): Unit = ???
//
//        def exceptAll(q: Query[M]): Query[M] = ???
//
//        def distinct: Query[M] = ???
//
//        def intersectAll(q: Query[M]): Query[M] = ???
//
//        def forUpdate: Query[M] = ???
//
//        def ast: O = ???
//
//        def statement: String = ???
//
//        private [squeryl] def copy(asRoot: Boolean, newUnions: List[(String, Query[M])]): Query[M] = ???
//
//        def intersect(q: Query[M]): Query[M] = ???
//
//        def unionAll(q: Query[M]): Query[M] = ???
//
//        protected[squeryl] def invokeYield(rsm: ResultSetMapper, resultSet: ResultSet): M = ???
//
//        def page(offset: Int, pageLength: Int): Query[M] = ???
//
//        def dumpAst: String = ???
//
//        def except(q: Query[M]): Query[M] = ???
//
//        def union(q: Query[M]): Query[M] = ???
//
//        private[squeryl] def give(resultSetMapper: ResultSetMapper, rs: ResultSet): M = ???
//
//        def name: String = ???
//
//        private[squeryl] def genericType: Class[M] = mClass.asInstanceOf[Class[M]]
//      }
//
//      def intercept(o: scala.Any, method: Method, args: Array[AnyRef], methodProxy: MethodProxy): AnyRef = {
//        interceptedBuffer.append(o, method)
//
//        val returnValue = methodProxy.invokeSuper(o, args)
//
//        if(returnValue != null && returnValue.getClass().getInterfaces().exists(c => c == classOf[OneToMany[_]])) {
//
//          def allInterfaces(i: Array[Class[_]]): Array[Class[_]] = {
//            val nestedInterfaces = i.flatMap(_.getInterfaces())
//            if(nestedInterfaces.nonEmpty)
//              i ++ allInterfaces(nestedInterfaces)
//            else
//              i
//          }
//
//          val interfaces = allInterfaces(returnValue.getClass().getInterfaces())
//          enhancer.setSuperclass(classOf[OneToManyProxy[_]])
//          val mClass = returnValue.asInstanceOf[OneToMany[_]].genericType
//          val constructors = classOf[OneToManyProxy[_]].getConstructors()
//          enhancer.create(Array(classOf[SampleLinkReader], classOf[Schema], classOf[Class[_]]), Array(this, schema, mClass))
////          enhancer.setInterfaces(Array(classOf[OneToMany[_]], classOf[Iterable[_]]))
//          val proxy = Try(enhancer.create())
//
//          proxy match {
//            case Success(x) => {
//              return x
//            }
//            case _ => {}
//          }
//        }
//
//        returnValue
//      }
//    }
}

class GroupQueryYield[K] (
   _qe: QueryElements[_],
   val groupByClauseClosure: ()=>List[TypedExpression[_,_]]
  )
  extends BaseQueryYield[Group[K]](_qe, null)
    with GroupByState[K]
    with HavingState[K]
    with OrderBySignatures[Group[K]]
    with QueryYield[Group[K]]
{

  override def groupByClause: List[ExpressionNode] =
    groupByClauseClosure().map(e => e)

  override def invokeYield(rsm: ResultSetMapper, rs: ResultSet): Group[K] =
    new Group(rsm.groupKeysMapper.get.mapToTuple(rs))

  override def queryElements =
    (whereClause, havingClause, groupByClause, orderByClause, commonTableExpressions)

  class SampleGroup[K](k:K)
    extends Group(k) {

    override def key = _sTuple1ToValue(k)
  }

  override def invokeYieldForAst(q: QueryExpressionNode[_], rsm: ResultSetMapper) = {
    val offset = 1
    val (m, nodes) = _createColumnToTupleMapper(q, groupByClauseClosure(), offset, true)
    rsm.groupKeysMapper = Some(m)
    val st = SampleTuple.create(nodes, m.outMappers).asInstanceOf[K]
    (nodes, new SampleGroup(st))
  }
}

class MeasuresQueryYield[M](
   _qe: QueryElements[_],
   _computeByClauseClosure: ()=>List[TypedExpression[_,_]]
  )
  extends BaseQueryYield[Measures[M]](_qe, null)
    with OrderBySignatures[Measures[M]]
    with ComputeStateStartOrWhereState[M]
    with QueryYield[Measures[M]]
{
  override def invokeYield(rsm: ResultSetMapper, rs: ResultSet): Measures[M] =
    new Measures(rsm.groupMeasuresMapper.get.mapToTuple(rs))

  override def queryElements =
    (whereClause, havingClause, groupByClause, orderByClause, commonTableExpressions)


  class SampleMeasures[M](m:M)
    extends Measures(m) {

    override def measures = _sTuple1ToValue(m)
  }

  override def invokeYieldForAst(q: QueryExpressionNode[_], rsm: ResultSetMapper) = {
    val offset = 1
    val (m, nodes) = _createColumnToTupleMapper(q, _computeByClauseClosure(), offset, false)
    rsm.groupMeasuresMapper = Some(m)
    val st = SampleTuple.create(nodes, m.outMappers).asInstanceOf[M]
    (nodes, new SampleMeasures(st))
  }
}

class GroupWithMeasuresQueryYield[K,M] (
  _qe: QueryElements[_],
  _groupByClauseClosure: ()=>List[TypedExpression[_,_]],
  _having: Option[()=>LogicalBoolean],
  _computeClauseClosure: ()=>List[TypedExpression[_,_]]
)
extends BaseQueryYield[GroupWithMeasures[K,M]](_qe, null)
  with ComputeStateFromGroupByState[K,M]
  with OrderBySignatures[GroupWithMeasures[K,M]]
  with QueryYield[GroupWithMeasures[K,M]]
{

  class SampleGroupWithMeasures[K, M](k:K, m:M)
    extends GroupWithMeasures(k,m) {

    override def key = _sTuple1ToValue(k)

    override def measures = _sTuple1ToValue(m)
  }

  override def havingClause =
    if(_having != None)
      _having.map(c=>c())
    else
      super.havingClause

   override def queryElements =
    (whereClause, havingClause, _groupByClauseClosure().map(e => e), orderByClause, commonTableExpressions)

  override def invokeYield(rsm: ResultSetMapper, rs: ResultSet) =
    new GroupWithMeasures(rsm.groupKeysMapper.get.mapToTuple(rs), rsm.groupMeasuresMapper.get.mapToTuple(rs))

  override def invokeYieldForAst(q: QueryExpressionNode[_], rsm: ResultSetMapper) = {

    val offset = 1

    val (km, knodes) = _createColumnToTupleMapper(q, _groupByClauseClosure(), offset, true)
    val (mm, mnodes) = _createColumnToTupleMapper(q, _computeClauseClosure(), offset + knodes.size, false)

    rsm.groupKeysMapper = Some(km)
    rsm.groupMeasuresMapper = Some(mm)

    val stK = SampleTuple.create(knodes, km.outMappers).asInstanceOf[K]
    val stM = SampleTuple.create(mnodes, mm.outMappers).asInstanceOf[M]

    (List(knodes,mnodes).flatten,  new SampleGroupWithMeasures(stK, stM))
  }
}

class IncludedPropertiesQueryYield[R](
                                       val qe: QueryElements[_],
                                       val sc: ()=>R,
                                       override val includeExpressions: Seq[(Seq[(JoinedQueryable[_], (Any, Any) => EqualityExpression)], (Any) => IncludePath[Any])])
  extends BaseQueryYield[R](qe, sc) {
}

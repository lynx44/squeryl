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
package org.squeryl.dsl

import ast._
import org.squeryl.dsl.internal.{JoinedQueryable, InnerJoinedQueryable, OuterJoinedQueryable}
import java.sql.ResultSet
import org.squeryl.dsl.fsm._
import org.squeryl.internals._
import org.squeryl._
import collection.mutable.ArrayBuffer
import org.squeryl.logging._
import java.io.Closeable

import scala.collection.immutable.HashMap

abstract class AbstractQuery[R](
    val isRoot:Boolean,
    private [squeryl] val unions: List[(String, Query[R])]
  ) extends Query[R] {

  private var subQueryableHashMap = new HashMap[Queryable[_], SubQueryable[_]]()
  private def createOrFindSubqueryable[U](q: Queryable[U]): SubQueryable[U] = {
    {if(subQueryableHashMap.contains(q)) {
      subQueryableHashMap(q)
    } else {
      val subQueryable = createSubQueryable(q)
      registerSubQueryable(q, subQueryable)
      subQueryable
    }}.asInstanceOf[SubQueryable[U]]
  }

  private def registerSubQueryable(q: Queryable[_], sq: SubQueryable[_]) = {
    subQueryableHashMap = subQueryableHashMap + (q -> sq)
  }

  protected lazy val includedSubQueryables: Iterable[SubQueryable[_]] = {
    joinedIncludes
    subQueryableHashMap.map(_._2).toList
  }

  protected lazy val joinedIncludes = sampleYield.includePath.map(includePathToJoinExpressionAdjacentRecursive(_))

  private def includePathToJoinExpressionAdjacentRecursive(left: IncludePathCommon): JoinedIncludePath = {
    val (leftTable, leftSubQueryable) = (left.table, createOrFindSubqueryable(left.table))
    val adjacentMembers =
      left.relations.filterNot(x => x.inhibited).map(includeRelation => {
        includeRelationToJoinIncludeRecursive(leftSubQueryable, left.classType.runtimeClass, includeRelation)
      })

    JoinedIncludePath(leftSubQueryable, None, None, None, adjacentMembers)
  }

  private def includeRelationToJoinIncludeRecursive(leftSubqueryable: SubQueryable[_], leftClass: Class[_], includeRelation: IncludePathRelation): JoinedIncludePath = {
    val right = includeRelation.right
    val rightTable = right.table
    val joinedQueryable: JoinedQueryable[_] =
      includeRelation match {
        case m: OneToManyIncludePathRelation[_, _] => new OuterJoinedQueryable[Any](rightTable.asInstanceOf[Queryable[Any]], "left")
        case m: ManyToOneIncludePathRelation[_, _] => new OuterJoinedQueryable[Any](rightTable.asInstanceOf[Queryable[Any]], "left")
      }
    val rightSubQueryable = createOrFindSubqueryable(joinedQueryable)
    val squerylRelation = includeRelation.equalityExpressionAccessor(leftSubqueryable.sample, rightSubQueryable.sample)

    val joinLogicalBoolean: () => LogicalBoolean = () => squerylRelation

    val relations =
      if(right.relations != null)
        right.relations.filterNot(x => x.inhibited).map(x => includeRelationToJoinIncludeRecursive(rightSubQueryable, right.classType.runtimeClass, x))
      else
        Seq()

    JoinedIncludePath(rightSubQueryable, Some(joinedQueryable), Some(joinLogicalBoolean), Some(includeRelation), relations)
  }

  case class JoinedIncludePath(subQueryable: SubQueryable[_],
                                joinedQueryable: Option[JoinedQueryable[_]],
                                joinCondition: Option[() => LogicalBoolean],
                                includePathRelation: Option[IncludePathRelation],
                                relations: Seq[JoinedIncludePath])

  def sampleYield: QueryYield[R]
  
  private [squeryl] var selectDistinct = false
  
  private [squeryl] var isForUpdate = false

  private [squeryl] var page: Option[(Int,Int)] = None

  private [squeryl] var unionIsForUpdate = false
  private [squeryl] var unionPage: Option[(Int, Int)] = None

  private var __root: Option[Query[R]] = None

  val resultSetMapper = new ResultSetMapper

  val name = "query"

  private def isUnionQuery = ! unions.isEmpty

  override private [squeryl] def root = __root

  def give(rsm: ResultSetMapper, rs: ResultSet): R = {
    rsm.pushYieldedValues(rs)
    val r = invokeYield(rsm, rs)
    r
  }

  /**
   * Builds the AST tree of the this Query, *some state mutation of the AST nodes occurs
   * during AST construction, for example, the parent child relationship is set by this method,
   * unique IDs of node that needs them for example.
   *
   * After this call, the query (and it's AST) becomes immutable by virtue of the unaccessibility
   * of it's public methods 
   */
  val definitionSite: Option[StackTraceElement] =
    if(!isRoot) None
    else Some(_deduceDefinitionSite)

  private def _deduceDefinitionSite: StackTraceElement = {
    val st = Thread.currentThread.getStackTrace
    var i = 1
    while(i < st.length) {
      val e = st(i)
      val cn = e.getClassName
      if((cn.startsWith("org.squeryl.") && (!cn.startsWith("org.squeryl.tests."))) || cn.startsWith("scala."))
        i = i + 1
      else
        return e
    }
    new StackTraceElement("unknown", "unknown", "unknown", -1)
  }

  protected def copyUnions(u: List[(String, Query[R])]) =
    u map (t => (t._1, t._2.copy(false, Nil)))

  protected def buildAst(qy: QueryYield[R], subQueryables: SubQueryable[_]*) = {

    subQueryables.foreach(s => registerSubQueryable(s.queryable, s))

    val subQueries = new ArrayBuffer[QueryableExpressionNode]

    val views = new ArrayBuffer[ViewExpressionNode[_]]

    val subQueryableCollection =
    if(joinedIncludes.nonEmpty) {
      val leftQuery = subQueryables.head

      def walkAndCollect(leftQueryable: SubQueryable[_], left: JoinedIncludePath, collected: Seq[(SubQueryable[_])]):
        Seq[(SubQueryable[_])] = {

        val newCollection =
          if(left.relations != null)
            left.relations.flatMap(walkAndCollect(left.subQueryable, _, collected))
          else
            Seq()

        if(left.joinedQueryable.nonEmpty) {
          qy.joinExpressions ++= Seq(left.joinCondition.get)
          left.subQueryable.node.joinExpression = Some(left.joinCondition.get())
          newCollection ++ collected ++ Seq(left.subQueryable)
        }
        else
          newCollection ++ collected
      }

      val subQueryableAndJoinClause = walkAndCollect(null, joinedIncludes.get, Seq()).reverse
      subQueryables ++ subQueryableAndJoinClause
    } else {
      subQueryables
    }

    if(qy.joinExpressions != Nil) {
      val sqIterator = subQueryableCollection.iterator
      val joinExprsIterator = qy.joinExpressions.iterator
      sqIterator.next // get rid of the first one

      while(sqIterator.hasNext) {
        val nthQueryable = sqIterator.next
        val nthJoinExpr = joinExprsIterator.next
        if(nthQueryable.node.joinExpression.isEmpty) {
          nthQueryable.node.joinExpression = Some(nthJoinExpr())
        }
      }
    }

    for(sq <- subQueryableCollection)
      if(! sq.isQuery)
        views.append(sq.node.asInstanceOf[ViewExpressionNode[_]])

    for(sq <- subQueryableCollection)
      if(sq.isQuery) {
        val z = sq.node.asInstanceOf[QueryExpressionNode[_]]
        if(! z.isUseableAsSubquery)
          org.squeryl.internals.Utils.throwError("Sub query returns a primitive type or a Tuple of primitive type, and therefore is not useable as a subquery in a from or join clause, see \nhttp://squeryl.org/limitations.html")
        subQueries.append(z)
      }

    val qen = new QueryExpressionNode[R](this, qy, subQueries, views)
    val (sl,d) = qy.invokeYieldForAst(qen, resultSetMapper)
    qen.setOutExpressionNodesAndSample(sl, d)

//    sl.filter(_.isInstanceOf[ExportedSelectElement]).
//       map(_.asInstanceOf[ExportedSelectElement]).
//       foreach(_.buildReferencePath)

    qen
  }

  def ast: QueryExpressionNode[R]

  def copy(asRoot:Boolean, newUnions: List[(String, Query[R])]) = {
    val c = createCopy(asRoot, newUnions)
    c.selectDistinct = selectDistinct
    c.page = page

    if (! isRoot) {
      c.__root = __root
    } else {
      c.__root = Some(this)
    }

    c
  }

  def createCopy(asRoot:Boolean, newUnions: List[(String, Query[R])]): AbstractQuery[R]

  def dumpAst = ast.dumpAst

  def statement: String = _genStatement(true)

  private def _genStatement(forDisplay: Boolean) = {

    val sw = new StatementWriter(forDisplay, Session.currentSession.databaseAdapter)
    ast.write(sw)
    sw.statement
  }

  def distinct = {
    if (isUnionQuery) {
      Utils.throwError("distinct is not supported on union queries")
    }
    val c = copy(true, Nil)
    c.selectDistinct = true;
    c
  }

  def page(offset: Int, pageLength: Int): Query[R] = {
    val c = copy(true, Nil)
    val page = Some((offset, pageLength))
    if (c.isUnionQuery)
      c.unionPage = page
    else
      c.page = page
    c    
  }

  def forUpdate = {
    val c = copy(true, Nil)
    if (c.isUnionQuery)
      c.unionIsForUpdate = true
    else
      c.isForUpdate = true;
    c    
  }

  private def _dbAdapter = Session.currentSession.databaseAdapter

  private class IncludeIterable[R](data: Seq[Seq[IncludeRowData]], columnDef: Seq[JoinedIncludePath]) extends Iterator[R] {
    private val structuredData: Seq[R] = {

      class DataColumn(data: Seq[IncludeRowData]) extends Iterable[IncludeRowData] {
        def iterator: Iterator[IncludeRowData] = data.iterator
      }

      case class GroupedData(keyValue: Option[Any], parentKeyValue: Option[Any], relation: Option[IncludePathRelation])

      if(data.nonEmpty) {
        val columnCount = data.head.length
        val columns = (0 to columnCount - 1).map(index => {
          data.map(_(index))
        })

        val groupedColumns = columns.map(i =>
          i.groupBy(g =>
            GroupedData(g.entity.map(_.id), g.parent.map(_.id), g.includePathRelation)))

        // gets the correct instance - the instance we're using for the specified key
        def findCanonicalRowData(d: Option[KeyedEntity[_]]): Option[KeyedEntity[_]] = {

          def findGroupColumn: Option[Map[GroupedData, Iterable[IncludeRowData]]] = {
            groupedColumns.filter(_.filter(_._2.filter(_.entity eq d).nonEmpty).nonEmpty).headOption
          }

          if(d.nonEmpty) {
            val groupColumn = findGroupColumn
            if(groupColumn.nonEmpty)
              return groupColumn.get.filter(_._2.filter(_.entity.map(_.id == d.get.id).getOrElse(false)).nonEmpty).head._2.head.entity
          }

          None
        }

        for (i <- 0 to columnCount - 1) {
          val columnGroup = groupedColumns(i)
          val keysAndIterableRow = columnGroup.map(g => (g._1, g._2.head))
          keysAndIterableRow.map(keyAndIterableRow => {
            val finalParent = findCanonicalRowData(keyAndIterableRow._2.parent)
            (keyAndIterableRow, finalParent)
          })
            .groupBy(_._2)
            .foreach(parentGroup => {
            val entities = parentGroup._2.map(x => findCanonicalRowData(x._1._2.entity))
            val fillEntities = entities.filterNot(z => z.isEmpty).map(_.get).toList

            val includeRelationOption = columnDef(i).includePathRelation //parentGroup._2.head._1._2._2.includePathRelation.get
            val parentData = parentGroup._1
            if(includeRelationOption.nonEmpty && parentData.nonEmpty) {
              val includeRelation = includeRelationOption.get
              includeRelation match {
                  case x: OneToManyIncludePathRelation[_, _] => {
                    val relationship = includeRelation.relationshipAccessor[StatefulOneToMany[Any]](parentData.get)
                    relationship.fill(fillEntities)
                  }
                  case y: ManyToOneIncludePathRelation[_, _] => {
                    val relationship = includeRelation.relationshipAccessor[StatefulManyToOne[Any]](parentData.get)
                    relationship.fill(fillEntities.headOption)
                  }
                }
            }
          })
        }

        groupedColumns.last.flatMap(x => x._2.map(y => findCanonicalRowData(y.entity).get.asInstanceOf[R])).toSeq
      } else {
        Seq()
      }
    }

    private val iterator = structuredData.iterator
    def hasNext: Boolean = iterator.hasNext

    def next(): R = iterator.next
  }

  private case class IncludeRowData(entity: Option[KeyedEntity[_]], parent: Option[KeyedEntity[_]], includePathRelation: Option[IncludePathRelation])

  def iterator = new Iterator[R] with Closeable {

    val sw = new StatementWriter(false, _dbAdapter)
    ast.write(sw)
    val s = Session.currentSession
    val beforeQueryExecute = System.currentTimeMillis
    val (rs, stmt) = _dbAdapter.executeQuery(s, sw)

    lazy val statEx = new StatementInvocationEvent(definitionSite.get, beforeQueryExecute, System.currentTimeMillis, -1, sw.statement)

    if(s.statisticsListener != None)
      s.statisticsListener.get.queryExecuted(statEx)

    s._addStatement(stmt) // if the iteration doesn't get completed, we must hang on to the statement to clean it up at session end.
    s._addResultSet(rs) // same for the result set
    
    var _nextCalled = false;
    var _hasNext = false;

    var rowCount = 0

    val includeIterable =
      if(sampleYield.includePath.nonEmpty)
      {
        def walkAndGetColumnLayout(left: JoinedIncludePath, collection: Seq[JoinedIncludePath]):
          Seq[JoinedIncludePath] = {

            val tempSeq = collection ++ Seq(left)
            val newCollection = left.relations.flatMap(walkAndGetColumnLayout(_, tempSeq))

            newCollection ++ tempSeq
        }

        val valueMap = Iterator.from(1).takeWhile(_ => rs.next).map(p => {
          def walkAndRead(left: JoinedIncludePath, parent: Option[KeyedEntity[_]], collection: Seq[IncludeRowData], isHead: Boolean = false):
            Seq[IncludeRowData] = {

              val value =
                if(!isHead)
                  left.subQueryable.give(rs).asInstanceOf[Option[KeyedEntity[_]]]
                else
                  Option(give(resultSetMapper, rs).asInstanceOf[KeyedEntity[_]])

              val tempSeq = collection ++ List(IncludeRowData(value, parent, left.includePathRelation))
              val newCollection = left.relations.flatMap(walkAndRead(_, value, tempSeq))

              newCollection ++ tempSeq
            }

            val row = walkAndRead(joinedIncludes.get, None, List(), true)
            row
          }
        ).toList
        Some(new IncludeIterable[R](valueMap, walkAndGetColumnLayout(joinedIncludes.get, List())))
      } else {
        None
      }

    
    def close {
      stmt.close
      rs.close
    }

    def _next = {
      _hasNext = rs.next

      if(!_hasNext) {// close it since we've completed the iteration
        Utils.close(rs)
        stmt.close

        if(s.statisticsListener != None) {
          s.statisticsListener.get.resultSetIterationEnded(statEx.uuid, System.currentTimeMillis, rowCount, true)
        }
      }
      
      rowCount = rowCount + 1
      _nextCalled = true
    }

    def hasNext = includeIterable.map(_.hasNext).getOrElse({
      if(!_nextCalled)
        _next
      _hasNext
    })

    def next: R = includeIterable.map(_.next).getOrElse({
      if(!_nextCalled)
        _next
      if(!_hasNext)
        throw new NoSuchElementException("next called with no rows available")
      _nextCalled = false

      if(s.isLoggingEnabled)
        s.log(ResultSetUtils.dumpRow(rs))

      give(resultSetMapper, rs)
    })
  }

  override def toString = dumpAst + "\n" + _genStatement(true)

  protected def createSubQueryable[U](q: Queryable[U]): SubQueryable[U] =
    if(q.isInstanceOf[View[_]]) {
      val v = q.asInstanceOf[View[U]]
      val vxn = v.viewExpressionNode
      vxn.sample =
        v.posoMetaData.createSample(FieldReferenceLinker.createCallBack(vxn))
      
      new SubQueryable(v, vxn.sample, vxn.resultSetMapper, false, vxn)
    }
    else if(q.isInstanceOf[OptionalQueryable[_]]) {
      val oqr = q.asInstanceOf[OptionalQueryable[U]]
      val sq = createSubQueryable[U](oqr.queryable)
      sq.node.inhibited = oqr.inhibited
      val oqCopy = new OptionalQueryable(sq.queryable)
      oqCopy.inhibited = oqr.inhibited
      new SubQueryable(oqCopy.asInstanceOf[Queryable[U]], Some(sq.sample).asInstanceOf[U], sq.resultSetMapper, sq.isQuery, sq.node)
    }
    else if(q.isInstanceOf[OuterJoinedQueryable[_]]) {
      val ojq = q.asInstanceOf[OuterJoinedQueryable[U]]
      val sq = createSubQueryable[U](ojq.queryable)
      sq.node.joinKind = Some((ojq.leftRightOrFull, "outer"))
      sq.node.inhibited = ojq.inhibited
      new SubQueryable(sq.queryable, Some(sq.sample).asInstanceOf[U], sq.resultSetMapper, sq.isQuery, sq.node)
    }
    else if(q.isInstanceOf[InnerJoinedQueryable[_]]) {
      val ijq = q.asInstanceOf[InnerJoinedQueryable[U]]
      val sq = createSubQueryable[U](ijq.queryable)
      sq.node.joinKind = Some((ijq.leftRightOrFull, "inner"))
      new SubQueryable(sq.queryable, sq.sample, sq.resultSetMapper, sq.isQuery, sq.node)
    }
    else if(q.isInstanceOf[DelegateQuery[_]]) {
      createSubQueryable(q.asInstanceOf[DelegateQuery[U]].q)
    }      
    else {
      val qr = q.asInstanceOf[AbstractQuery[U]]
      val copy = qr.copy(false, Nil)
      new SubQueryable(copy, copy.ast.sample.asInstanceOf[U], copy.resultSetMapper, true, copy.ast)
    }

  protected class SubQueryable[U]
    (val queryable: Queryable[U],
     val sample: U,
     val resultSetMapper: ResultSetMapper,
     val isQuery:Boolean,
     val node: QueryableExpressionNode) {

    def give(rs: ResultSet): U =
      if(node.joinKind != None) {
        if(node.isOuterJoined) {

          val isNoneInOuterJoin =
            (!isQuery) && resultSetMapper.isNoneInOuterJoin(rs)

           if(isNoneInOuterJoin)
             None.asInstanceOf[U]
           else
             Some(queryable.give(resultSetMapper, rs)).asInstanceOf[U]
        }
        else
          queryable.give(resultSetMapper, rs)
      }
      else if((node.isRightJoined) && resultSetMapper.isNoneInOuterJoin(rs))
        sample
      else
        queryable.give(resultSetMapper, rs)
  }

  private def createUnion(kind: String, q: Query[R]): Query[R] = 
    copy(true, List((kind, q)))

  def union(q: Query[R]): Query[R] = createUnion("Union", q) 

  def unionAll(q: Query[R]): Query[R] = createUnion("Union All", q)

  def intersect(q: Query[R]): Query[R] = createUnion("Intersect", q)

  def intersectAll(q: Query[R]): Query[R] = createUnion("Intersect All", q)

  def except(q: Query[R]): Query[R] = createUnion("Except", q)

  def exceptAll(q: Query[R]): Query[R] = createUnion("Except All", q)
}

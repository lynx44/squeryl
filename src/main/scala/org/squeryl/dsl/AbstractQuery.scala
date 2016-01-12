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
import scala.reflect.ClassTag

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
      }).toList

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
        right.relations.filterNot(x => x.inhibited).map(x => includeRelationToJoinIncludeRecursive(rightSubQueryable, right.classType.runtimeClass, x)).toList
      else
        List()

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
            List()

        if(left.joinedQueryable.nonEmpty) {
          qy.joinExpressions ++= List(left.joinCondition.get)
          left.subQueryable.node.joinExpression = Some(left.joinCondition.get())
          newCollection ++ collected ++ List(left.subQueryable)
        }
        else
          newCollection ++ collected
      }

      val startTime = System.nanoTime()
      val subQueryableAndJoinClause = walkAndCollect(null, joinedIncludes.get, List()).reverse
      val endTime = System.nanoTime()
      val elapsed = endTime - startTime
      output(s"subqueryable join clause: $elapsed ns")
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

  case class GroupedData(keyValue: Option[Any], parentKeyValue: Option[Any], relation: Option[IncludePathRelation])

  private case class IncludeRowData(entity: Option[KeyedEntity[_]], parent: Option[KeyedEntity[_]], includePathRelation: Option[IncludePathRelation])

  def iterator = if(sampleYield.includePath.nonEmpty) {
      new IncludeIterator
    } else {

  new Iterator[R] with Closeable {
    val iteratorStartTime = System.nanoTime()
    val sw = new StatementWriter(false, _dbAdapter)
    ast.write(sw)
    val s = Session.currentSession
    val beforeQueryExecute = System.currentTimeMillis
    val queryStartTime = System.nanoTime()
    val (rs, stmt) = _dbAdapter.executeQuery(s, sw)
    output(s"dbAdapter.executeQuery: ${System.nanoTime() - queryStartTime} ns")

    lazy val statEx = new StatementInvocationEvent(definitionSite.get, beforeQueryExecute, System.currentTimeMillis, -1, sw.statement)

    if(s.statisticsListener != None)
      s.statisticsListener.get.queryExecuted(statEx)

    s._addStatement(stmt) // if the iteration doesn't get completed, we must hang on to the statement to clean it up at session end.
    s._addResultSet(rs) // same for the result set
    
    var _nextCalled = false;
    var _hasNext = false;

    var rowCount = 0

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

    def hasNext = {
      if(!_nextCalled)
        _next
      _hasNext
    }

    def next: R = {
      if(!_nextCalled)
        _next
      if(!_hasNext)
        throw new NoSuchElementException("next called with no rows available")
      _nextCalled = false

      if(s.isLoggingEnabled)
        s.log(ResultSetUtils.dumpRow(rs))

      give(resultSetMapper, rs)
    }
  }
}

  private class IncludeIterator extends Iterator[R] with Closeable {
    val startTime = System.nanoTime()
    val sw = new StatementWriter(false, _dbAdapter)
    ast.write(sw)
    val s = Session.currentSession
    val beforeQueryExecute = System.currentTimeMillis
    val queryStartTime = System.nanoTime()
    val (rs, stmt) = _dbAdapter.executeQuery(s, sw)
    output(s"dbAdapter.executeQuery: ${System.nanoTime() - queryStartTime} ns")

    lazy val statEx = new StatementInvocationEvent(definitionSite.get, beforeQueryExecute, System.currentTimeMillis, -1, sw.statement)

    if(s.statisticsListener != None)
      s.statisticsListener.get.queryExecuted(statEx)

    s._addStatement(stmt) // if the iteration doesn't get completed, we must hang on to the statement to clean it up at session end.
    s._addResultSet(rs) // same for the result set

    output(s"sql total elapsed: ${System.nanoTime() - startTime} ns")

    val cachedReadStopwatch = new Stopwatch()
    class EntityCache {

      val cachedEntityData: collection.mutable.LongMap[collection.mutable.LongMap[Option[KeyedEntity[_]]]] = new collection.mutable.LongMap[collection.mutable.LongMap[Option[KeyedEntity[_]]]]()
      def cacheEntity(includePath: JoinedIncludePath, primaryKey: Any, entity: Option[KeyedEntity[_]]) = {
        val cachedTableMap = cachedEntityData.get(includePath.hashCode())
        val tableMap =
          cachedTableMap.fold({
            val newMap = collection.mutable.LongMap[Option[KeyedEntity[_]]]()
            cachedEntityData.put(includePath.hashCode(), newMap)

            newMap
          })(a => a)

        tableMap += ((primaryKey.hashCode(), entity))
      }

      private val defaultKeyEntityMap = collection.mutable.LongMap[Option[KeyedEntity[_]]]()
      def getCachedEntity(includePath: JoinedIncludePath, primaryKey: Any): Option[KeyedEntity[_]] = {
        cachedReadStopwatch.start
        val options = {
          val e1 = cachedEntityData.get(includePath.hashCode())

//          if(e1 == null) defaultKeyEntityMap else e1
          e1.getOrElse(defaultKeyEntityMap)
        }
        val value = {
          val e2 = options.getOrElse(primaryKey.hashCode(), None)
          e2
        }
        cachedReadStopwatch.stop
        value
      }
    }

    val entityCache = new EntityCache


    val readStopwatch = new Stopwatch()
    val readPkStopwatch = new Stopwatch()
    val populateRelationsStopwatch = new Stopwatch()
    val getFieldMetadataStopwatch = new Stopwatch()
    val parentPkReaderStopwatch = new Stopwatch()
    val materializeStopwatch = new Stopwatch()
    val materializeParentStopwatch = new Stopwatch()
    val getEntityStopwatch = new Stopwatch()
    val parentPkMetadataFieldPath = collection.mutable.LongMap[FieldMetaData]()
    val materializeCounter = new Counter()
    val cacheHitCounter = new Counter()
    readStopwatch.start
    Iterator.from(1).takeWhile(_ => rs.next).foreach(p => {

      case class ReadRow(entity: KeyedEntity[_], parentPk: Seq[Any])
      def readRow(includePath: JoinedIncludePath, parentIncludePath: Option[JoinedIncludePath]): Option[ReadRow] = {

        val children = includePath.relations.map(r => {
          (r, readRow(r, Some(includePath)))
        })

        readPkStopwatch.start
        val primaryKey = children.filter(_._1.includePathRelation.map(_.isInstanceOf[OneToManyIncludePathRelation[_, _]]).getOrElse(false)).flatMap(_._2).headOption.fold({
          val key = includePath.subQueryable.resultSetMapper.readPrimaryKey(rs)
          if(key.isEmpty) {
            val sampleId = includePath.subQueryable.sample.asInstanceOf[Option[KeyedEntity[_]]].get.id
            if(sampleId.isInstanceOf[CompositeKey]) {
              includePath.subQueryable.resultSetMapper.readFields(sampleId.asInstanceOf[CompositeKey]._fields, rs)
            } else {
              key
            }
          } else {
            key
          }
        })(child => child.parentPk)
        readPkStopwatch.stop

        getEntityStopwatch.start
        val keyedEntity = if(primaryKey.nonEmpty) {
          val cachedEntity = entityCache.getCachedEntity(includePath, primaryKey)

          if(cachedEntity.nonEmpty) {
            cacheHitCounter.increment
            cachedEntity
          } else {
            val materializedEntity =
            if(parentIncludePath.nonEmpty) {
              materializeCounter.increment
              materializeStopwatch.measure(
              includePath.subQueryable.give(rs).asInstanceOf[Option[KeyedEntity[_]]]
              )
            } else {
              materializeCounter.increment
              materializeParentStopwatch.measure(
                Option(includePath.subQueryable.give(rs).asInstanceOf[KeyedEntity[R]])
              )
            }

            entityCache.cacheEntity(includePath, primaryKey, materializedEntity)

            materializedEntity
          }
        } else {
          None
        }
        getEntityStopwatch.stop

        populateRelationsStopwatch.start
        if(keyedEntity.nonEmpty) {
          children.foreach(c => {
            c._1.includePathRelation.map(includeChildRelation => {
              includeChildRelation match {
                case x: OneToManyIncludePathRelation[_, _] => {
                  val relationship = includeChildRelation.relationshipAccessor[StatefulOneToMany[Any]](keyedEntity.get)
                  relationship.add(c._2.map(_.entity))
                }
                case y: ManyToOneIncludePathRelation[_, _] => {
                  val relationship = includeChildRelation.relationshipAccessor[StatefulManyToOne[Any]](keyedEntity.get)
                  relationship.fill(c._2.map(_.entity))
                }
              }
            })
          })
        }
        populateRelationsStopwatch.stop

        getFieldMetadataStopwatch.start
        val parentPkFieldMetaData = parentIncludePath.map(parentInclude =>
          getParentPKFieldMetadata(includePath, parentInclude.subQueryable.sample, includePath.subQueryable.sample))
        getFieldMetadataStopwatch.stop

        keyedEntity.map(entity => {
          parentPkReaderStopwatch.start
          val pk = parentPkFieldMetaData.map(_.get(entity))
          parentPkReaderStopwatch.stop
          ReadRow(entity, pk.toList)
        })
      }

      def getParentPKFieldMetadata(includePath: JoinedIncludePath, parentSample: Any, currentSample: Any): FieldMetaData = {
        parentPkMetadataFieldPath.get(includePath.hashCode()).fold({
          val accessor = includePath.includePathRelation.head.equalityExpressionAccessor(parentSample, currentSample)
          val fmd = if(includePath.includePathRelation.map(_.isInstanceOf[OneToManyIncludePathRelation[_, _]]).getOrElse(false)) {
            accessor.right._fieldMetaData
          } else {
            accessor.left._fieldMetaData
          }

          parentPkMetadataFieldPath += ((includePath.hashCode(), fmd))
          fmd
        })(fmd => fmd)
      }

      readRow(joinedIncludes.get, None)
    })

    readStopwatch.stop

    output(s"materializeStopwatch: ${materializeStopwatch.elapsed} ns")
    output(s"materializeParentStopwatch: ${materializeParentStopwatch.elapsed} ns")
    output(s"materializeCounter: ${materializeCounter.count}")
    output(s"cachedReadStopwatch: ${cachedReadStopwatch.elapsed} ns")
    output(s"cacheHitCounter: ${cacheHitCounter.count}")
    output()

    output(s"readPkStopwatch: ${readPkStopwatch.elapsed} ns")
    output(s"getEntityStopwatch: ${getEntityStopwatch.elapsed} ns")
    output(s"populateRelationsStopwatch: ${populateRelationsStopwatch.elapsed} ns")
    output(s"getFieldMetadataStopwatch: ${getFieldMetadataStopwatch.elapsed} ns")
    output(s"parentPkReaderStopwatch: ${parentPkReaderStopwatch.elapsed} ns")

    val topLevelStopwatches = Seq(readPkStopwatch, getEntityStopwatch, populateRelationsStopwatch, getFieldMetadataStopwatch, parentPkReaderStopwatch)

    output(s"stopwatches total: ${topLevelStopwatches.map(_.elapsed).sum} ns")
    output()

    output(s"walkAndRead: ${readStopwatch.elapsed} ns")
    output(s"walkAndRead total elapsed: ${System.nanoTime() - startTime} ns")

    private val entityList = joinedIncludes.fold(
      List[R]()
    )(ji => {
      entityCache.cachedEntityData.get(ji.hashCode()).fold(List[R]())(b => b.map(_._2).flatMap(_.asInstanceOf[Option[R]]).toList)
    })

    output(s"entityList total count: ${entityList.length}")
    output(s"entityList total elapsed: ${System.nanoTime() - startTime} ns")

    private val iterator = entityList.iterator

    def hasNext: Boolean = iterator.hasNext

    def next(): R = iterator.next()

    def close(): Unit = {
      stmt.close
      rs.close
    }
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
  
  def output(log: String = ""): Unit = {
    println(log)
  }
}

class Stopwatch {
  private var _elapsed = 0L
  private var _lastStart = 0L

  def elapsed = _elapsed

  def start: Unit = {
    _lastStart = System.nanoTime()
  }

  def stop: Unit = {
    _elapsed += System.nanoTime() - _lastStart
    _lastStart = 0
  }

  def measure[A](block: => A): A = {
    start
    val a = block
    stop
    a
  }
}

class Counter {
  private var _count = 0L

  def count = _count

  def increment: Unit = {
    _count += 1
  }
}
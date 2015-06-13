package org.squeryl.test

import org.squeryl.dsl.OneToMany
import org.squeryl.{Schema, KeyedEntity}
import org.squeryl.framework.{DBConnector, DbTestBase}
import org.squeryl.test.PrimitiveTypeModeForTests._

class Person(val lastName: String) extends KeyedEntity[Long] {
  val id: Long = 0

  lazy val children = IncludeSchema.foo_bar_relation.left(this)
}

class Child(val name: String, val personId: Long) extends KeyedEntity[Long] {
  val id: Long = 0
}

object IncludeSchema extends Schema {
  val people = table[Person]
  val children = table[Child]

  val foo_bar_relation = oneToManyRelation(people, children).via((f, b) => f.id === b.personId)

  def reset() = {
    drop // its protected for some reason
    create
  }
}

class IncludeTest extends DbTestBase {
  self: DBConnector =>

//  test("basic query") {
//    implicit val schema = IncludeSchema
//    transaction {
//      IncludeSchema.reset
//    }
//
//    transaction {
//      val p1 = IncludeSchema.people.insert(new Person("person1"))
//      val p2 = IncludeSchema.people.insert(new Person("person2"))
//
//      val data = from(IncludeSchema.people)(p => select(p))
//
//      data.last
//      data.head
//    }
//
//
////    assert(data.size == 2)
//  }

  test("include oneToMany - many relation with one child") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val p = IncludeSchema.people.insert(new Person("person"))
      val c = IncludeSchema.children.insert(new Child("child", p.id))

      from(IncludeSchema.people)(p => select(p) include(d => d.children)).head
    }

    assert(data.children.size == 1)
  }

  test("include oneToMany - many relation with two children") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val p = IncludeSchema.people.insert(new Person("person"))
      val c1 = IncludeSchema.children.insert(new Child("child1", p.id))
      val c2 = IncludeSchema.children.insert(new Child("child2", p.id))

      from(IncludeSchema.people)(p => select(p) include(d => d.children)).head
    }

    assert(data.children.size == 2)
  }
}


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

  test("include oneToMany - many relation with two parents and two children") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val p1 = IncludeSchema.people.insert(new Person("person1"))
      val p2 = IncludeSchema.people.insert(new Person("person2"))
      val c1 = IncludeSchema.children.insert(new Child("child1", p1.id))
      val c2 = IncludeSchema.children.insert(new Child("child2", p2.id))

      from(IncludeSchema.people)(p => select(p) include(d => d.children)).toList
    }

    assert(data.size == 2)
    assert(data.filter(p => p.lastName == "person1").head.children.head.name == "child1")
    assert(data.filter(p => p.lastName == "person2").head.children.head.name == "child2")
  }

  test("include oneToMany - many relation with no data returns empty") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      from(IncludeSchema.people)(p => select(p) include(d => d.children)).toList
    }

    assert(data.size == 0)
  }

  test("include oneToMany - can iterate included property multiple times") {
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
    assert(data.children.size == 1)
  }

  test("include oneToMany - many relation with no children returns empty") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val p = IncludeSchema.people.insert(new Person("person"))

      from(IncludeSchema.people)(p => select(p) include(d => d.children)).head
    }

    assert(data.children.size == 0)
  }

  test("include oneToMany - can delete children") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    transaction {
      val p = IncludeSchema.people.insert(new Person("person"))
      val c = IncludeSchema.children.insert(new Child("child", p.id))

      val data = from(IncludeSchema.people)(p => select(p) include(d => d.children)).head

      data.children.deleteAll

      assert(data.children.size == 0)
    }
  }

  test("include oneToMany - can associate children") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    transaction {
      val p = IncludeSchema.people.insert(new Person("person"))
      val c = new Child("child", p.id)

      val data = from(IncludeSchema.people)(p => select(p) include(d => d.children)).head

      data.children.associate(c)

      assert(data.children.size == 1)
    }
  }

  test("include oneToMany - can assign children") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val (p, data) = transaction {
      val p = IncludeSchema.people.insert(new Person("person"))

      (p, from(IncludeSchema.people)(p => select(p) include(d => d.children)).head)
    }
    val c = new Child("child", p.id)

    data.children.assign(c)
  }
}


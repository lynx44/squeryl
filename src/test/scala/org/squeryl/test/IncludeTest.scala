package org.squeryl.test

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

  test("include oneToMany - many relation") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val foo1 = IncludeSchema.people.insert(new Person("test"))
      val bar1 = IncludeSchema.children.insert(new Child("test", foo1.id))

      join(IncludeSchema.people, IncludeSchema.children.leftOuter)((f,b) =>
        select(f, b)
          on(f.id === b.map(_.personId))
      ).head
      from(IncludeSchema.people)(p => select(p) include(p.children)).statement
    }

//    assert(data.children.size == 1)
    assert("" == data)
  }
}


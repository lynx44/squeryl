package org.squeryl.test

import org.squeryl.framework.{DBConnector, DbTestBase}
import org.squeryl.test.PrimitiveTypeModeForTests._
import org.squeryl.{KeyedEntity, Schema}

class Manager(val lastName: String) extends KeyedEntity[Long] {
  val id: Long = 0

  lazy val employees = IncludeSchema.manager_employee_relation.left(this)
  lazy val responsibilities = IncludeSchema.manager_responsibility_relation.left(this)
}

class Employee(val name: String, val managerId: Long) extends KeyedEntity[Long] {
  val id: Long = 0
}

class Responsibility(val name: String, val managerId: Long) extends KeyedEntity[Long] {
  val id: Long = 0
}

object IncludeSchema extends Schema {
  val managers = table[Manager]
  val employees = table[Employee]
  val responsibilities = table[Responsibility]

  val manager_employee_relation = oneToManyRelation(managers, employees).via((m, e) => m.id === e.managerId)
  val manager_responsibility_relation = oneToManyRelation(managers, responsibilities).via((m, r) => m.id === r.managerId)

  def reset() = {
    drop // its protected for some reason
    create
  }
}

class IncludeTest extends DbTestBase {
  self: DBConnector =>

  //TODO: composite key test
  //TODO: nested include test? select(p) include(d => d.employees.map(_.responsibilities))
  //TODO: multiple from clauses
  //TODO: joinsmad

  // wish list
  // TODO: where(p.employees.map(_.name === "bob")) select(p) include(d => d.employees)

  // Single Include tests

  test("include oneToMany - many relation with one child") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val p = IncludeSchema.managers.insert(new Manager("person"))
      val c = IncludeSchema.employees.insert(new Employee("child", p.id))

      from(IncludeSchema.managers)(p => select(p) include(d => d.employees)).head
    }

    assert(data.employees.size == 1)
  }

  test("include oneToMany - many relation with two children") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val p = IncludeSchema.managers.insert(new Manager("person"))
      val c1 = IncludeSchema.employees.insert(new Employee("child1", p.id))
      val c2 = IncludeSchema.employees.insert(new Employee("child2", p.id))

      from(IncludeSchema.managers)(p => select(p) include(d => d.employees)).head
    }

    assert(data.employees.size == 2)
  }

  test("include oneToMany - many relation with two parents and two children") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val p1 = IncludeSchema.managers.insert(new Manager("person1"))
      val p2 = IncludeSchema.managers.insert(new Manager("person2"))
      val c1 = IncludeSchema.employees.insert(new Employee("child1", p1.id))
      val c2 = IncludeSchema.employees.insert(new Employee("child2", p2.id))

      from(IncludeSchema.managers)(p => select(p) include(d => d.employees)).toList
    }

    assert(data.size == 2)
    assert(data.filter(p => p.lastName == "person1").head.employees.head.name == "child1")
    assert(data.filter(p => p.lastName == "person2").head.employees.head.name == "child2")
  }

  test("include oneToMany - many relation with no data returns empty") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      from(IncludeSchema.managers)(p => select(p) include(d => d.employees)).toList
    }

    assert(data.size == 0)
  }

  test("include oneToMany - can iterate included property multiple times") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val p = IncludeSchema.managers.insert(new Manager("person"))
      val c = IncludeSchema.employees.insert(new Employee("child", p.id))

      from(IncludeSchema.managers)(p => select(p) include(d => d.employees)).head
    }

    assert(data.employees.size == 1)
    assert(data.employees.size == 1)
  }

  test("include oneToMany - many relation with no children returns empty") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val p = IncludeSchema.managers.insert(new Manager("person"))

      from(IncludeSchema.managers)(p => select(p) include(d => d.employees)).head
    }

    assert(data.employees.size == 0)
  }

  test("include oneToMany - can delete children") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    transaction {
      val p = IncludeSchema.managers.insert(new Manager("person"))
      val c = IncludeSchema.employees.insert(new Employee("child", p.id))

      val data = from(IncludeSchema.managers)(p => select(p) include(d => d.employees)).head

      data.employees.deleteAll

      assert(data.employees.size == 0)
    }
  }

  test("include oneToMany - can associate children") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    transaction {
      val p = IncludeSchema.managers.insert(new Manager("person"))
      val c = new Employee("child", p.id)

      val data = from(IncludeSchema.managers)(p => select(p) include(d => d.employees)).head

      data.employees.associate(c)

      assert(data.employees.size == 1)
    }
  }

  test("include oneToMany - can assign children") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val (p, data) = transaction {
      val p = IncludeSchema.managers.insert(new Manager("person"))

      (p, from(IncludeSchema.managers)(p => select(p) include(d => d.employees)).head)
    }
    val c = new Employee("child", p.id)

    data.employees.assign(c)
  }

  // end Single Include tests

  // Double Include tests

  test("include oneToMany - many relation with two includes, one child each") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val p = IncludeSchema.managers.insert(new Manager("person"))
      val c = IncludeSchema.employees.insert(new Employee("child", p.id))
      val r = IncludeSchema.responsibilities.insert(new Responsibility("r", p.id))

      from(IncludeSchema.managers)(p => select(p) include(d => d.responsibilities) include(d => d.employees)).head
    }

    assert(data.responsibilities.size == 1)
    assert(data.employees.size == 1)
  }

  // end Double Include tests
}


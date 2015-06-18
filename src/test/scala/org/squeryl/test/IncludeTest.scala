package org.squeryl.test

import org.squeryl.dsl.fsm.Path
import org.squeryl.dsl.fsm.Path._
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

  lazy val benefits = IncludeSchema.employee_benefit_relation.left(this)
}

class Responsibility(val name: String, val managerId: Long) extends KeyedEntity[Long] {
  val id: Long = 0

  lazy val types = IncludeSchema.responsibility_responsibilityType_relation.left(this)
}

class ResponsibilityType(val name: String, val responsibilityId: Long) extends KeyedEntity[Long] {
  val id: Long = 0
}

class Benefit(val name: String, val employeeId: Long) extends KeyedEntity[Long] {
  val id: Long = 0

  lazy val categories = IncludeSchema.benefit_category_relation.left(this)
}

class Category(val name: String, val benefitId: Long) extends KeyedEntity[Long] {
  val id: Long = 0
}

object IncludeSchema extends Schema {
  val managers = table[Manager]
  val employees = table[Employee]
  val responsibilities = table[Responsibility]
  val responsibilityTypes = table[ResponsibilityType]
  val benefits = table[Benefit]
  val categories = table[Category]

  val manager_employee_relation = oneToManyRelation(managers, employees).via((m, e) => m.id === e.managerId)
  val manager_responsibility_relation = oneToManyRelation(managers, responsibilities).via((m, r) => m.id === r.managerId)
  val responsibility_responsibilityType_relation = oneToManyRelation(responsibilities, responsibilityTypes).via((r, t) => r.id === t.responsibilityId)
  val employee_benefit_relation = oneToManyRelation(employees, benefits).via((e, b) => e.id === b.employeeId)
  val benefit_category_relation = oneToManyRelation(benefits, categories).via((b, c) => b.id === c.benefitId)

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
  //TODO: joins
  //TODO: many to many
  //TODO: many to one

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

      from(IncludeSchema.managers)(p => select(p) include(p->>(_.->(_.employees)))).head
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

      from(IncludeSchema.managers)(p => select(p) include(p->>(_.->(_.employees)))).head
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

      from(IncludeSchema.managers)(p => select(p) include(p->>(_.->(_.employees)))).toList
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
      from(IncludeSchema.managers)(p => select(p) include(p->>(_.->(_.employees)))).toList
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

      from(IncludeSchema.managers)(p => select(p) include(p->>(_.->(_.employees)))).head
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

      from(IncludeSchema.managers)(p => select(p) include(p->>(_.->(_.employees)))).head
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

      val data = from(IncludeSchema.managers)(p => select(p) include(p->>(_.->(_.employees)))).head

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

      val data = from(IncludeSchema.managers)(p => select(p) include(p->>(_.->(_.employees)))).head

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

      //enclosing => adjacent => onetomany
      (p, from(IncludeSchema.managers)(p => select(p) include(p->>(_.->(_.employees)))).head)
    }
    val c = new Employee("child", p.id)

    data.employees.assign(c)
  }

//   end Single Include tests

  // begin Nested Include tests

  test("include oneToMany - can retrieve two adjacent properties") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val m = IncludeSchema.managers.insert(new Manager("person"))
      val e = IncludeSchema.employees.insert(new Employee("child", m.id))
      val r = IncludeSchema.responsibilities.insert(new Responsibility("responsibility", m.id))

      from(IncludeSchema.managers)(p => select(p)
      include(p.->>(_.->(_.employees), _.->(_.responsibilities)))).head
    }

    assert(data.employees.size == 1)
    assert(data.responsibilities.size == 1)
  }

  test("include oneToMany - can retrieve two nested properties") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val m = IncludeSchema.managers.insert(new Manager("person"))
      val e = IncludeSchema.employees.insert(new Employee("child", m.id))
      val r = IncludeSchema.benefits.insert(new Benefit("benefit", e.id))

      from(IncludeSchema.managers)(p => select(p)
      include(p.->>(_.->(_.employees).->(_.benefits)))).head
    }

    assert(data.employees.size == 1)
    assert(data.employees.head.benefits.size == 1)
  }

  test("include oneToMany - can retrieve two nested properties with correct assignments") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val m = IncludeSchema.managers.insert(new Manager("person"))
      val e1 = IncludeSchema.employees.insert(new Employee("child1", m.id))
      val b1 = IncludeSchema.benefits.insert(new Benefit("benefit1", e1.id))
      val e2 = IncludeSchema.employees.insert(new Employee("child2", m.id))
      val b2 = IncludeSchema.benefits.insert(new Benefit("benefit2", e2.id))

      from(IncludeSchema.managers)(p => select(p)
      include(p.->>(_.->(_.employees).->(_.benefits)))).head
    }

    assert(data.employees.size == 2)
    assert(data.employees.head.benefits.size == 1)
    assert(data.employees.head.benefits.head.name == "benefit1")
    assert(data.employees.last.benefits.size == 1)
    assert(data.employees.last.benefits.head.name == "benefit2")
  }

  test("include oneToMany - can retrieve nested properties on adjacent properties with correct assignments") {
    implicit val schema = IncludeSchema
    transaction {
      IncludeSchema.reset
    }

    val data = transaction {
      val m = IncludeSchema.managers.insert(new Manager("person"))
      val e = IncludeSchema.employees.insert(new Employee("employee", m.id))
      val b = IncludeSchema.benefits.insert(new Benefit("benefit", e.id))
      val r = IncludeSchema.responsibilities.insert(new Responsibility("responsibility", m.id))
      val rt = IncludeSchema.responsibilityTypes.insert(new ResponsibilityType("responsibilityType", r.id))

      from(IncludeSchema.managers)(p => select(p)
      include(p.->>(_.->(_.employees).->(_.benefits), _.->(_.responsibilities).->(_.types)))).head
    }

    assert(data.employees.size == 1)
    assert(data.employees.head.benefits.size == 1)
    assert(data.responsibilities.size == 1)
    assert(data.responsibilities.head.types.size == 1)
  }

  //end Nested Include tests
}


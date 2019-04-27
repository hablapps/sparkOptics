[![Build Status](https://travis-ci.com/hablapps/sparkOptics.svg?token=pvJZNjJ8hxxoMyPVvQ8u&branch=master)](https://travis-ci.com/hablapps/sparkOptics)
[![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/spark-optics_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/org.hablapps/spark-optics_2.11)
[![Gitter](https://badges.gitter.im/hablapps/sparkOptics.svg)](https://gitter.im/hablapps/sparkOptics?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/hablapps/sparkOptics/binder?filepath=%2Fnotebooks%2FSparkLenses.ipynb)

# Spark-optics
Modify your complex structures in spark-sql dataframes with optics.

## Getting Started

Need to set an inner element in a complex structure?

```scala
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
val df: DataFrame = ???

import org.hablapps.sparkOptics._
df.select(Lens("field.subfield")(df.schema).set(lit(13)):_*)
```

Want to try it right now, click on the binder icon to lunch a interactive notebook.

### Installing

Compiled for scala 2.11 only. Tested with spark 2.3 and 2.4

```sbtshell
libraryDependencies += "org.hablapps" %% "spark-optics" % "1.0.0"
```

Spark lens doesn't have any dependencies beyond Spark itself.

### Implemented optics

#### Lens
Optic to focus in a column of a provided schema.

#### Protolens
Optic equivalent to a lens, but without an specific schema, so that it can be applied to any dataframe that contains the specified column.

### Motivation and larger example
Working with complex structures in Spark Sql's DataFrames can be hard. 
A common case if you are working with complex structures is to modify the inner elements of the structure. For instance:

```scala
case class Street(number: Int, name: String)
case class Address(city: String, street: Street)
case class Company(name: String, address: Address)
case class Employee(name: String, company: Company)

val employee = Employee("john", Company("awesome inc", Address("london", Street(23, "high street"))))
val df = List(employee).toDS.toDF
```
```
root
 |-- name: string (nullable = true)
 |-- company: struct (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- address: struct (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- street: struct (nullable = true)
 |    |    |    |-- number: integer (nullable = false)
 |    |    |    |-- name: string (nullable = true)
 ```
 
In order to modify an inner element, like changing the name of the street, we need to do something like this:

```scala
val mDF = df.select(df("name"),struct(
   df("company.name").as("name"),
   struct(
     df("company.address.city").as("city"),
     struct(
       df("company.address.street.number").as("number"),
       upper(df("company.address.street.name")).as("name")
     ).as("street")
   ).as("address")
 ).as("company"))
mDF.printSchema
val longCodeEmployee = mDF.as[Employee].head
```
```
root
  |-- name: string (nullable = true)
  |-- company: struct (nullable = false)
  |    |-- name: string (nullable = true)
  |    |-- address: struct (nullable = false)
  |    |    |-- city: string (nullable = true)
  |    |    |-- street: struct (nullable = false)
  |    |    |    |-- number: integer (nullable = true)
  |    |    |    |-- name: string (nullable = true)
 
mDF: DataFrame = [name: string, company: struct<name: string, address: struct<city: string, street: struct<number: int, name: string>>>]
longCodeEmployee: Employee = Employee(
"john",
Company("awesome inc", Address("london", Street(23, "HIGH STREET"))))
```
 
This can be simplified by using spark-optics. It allows you to focus in the element that you want to modify,
and the optics will recreate the structure for you.

```scala
import org.hablapps.sparkOptics._
import org.apache.spark.sql.functions._

val df: DataFrame = List(employee).toDF

val streetNameLens = Lens("company.address.street.name")(df.schema)
val modifiedDF = df.select(streetNameLens.modify(upper):_*)
modifiedDF.printSchema
modifiedDF.as[Employee].head
```
```
root
|-- name: string (nullable = true)
|-- company: struct (nullable = false)
|    |-- name: string (nullable = true)
|    |-- address: struct (nullable = false)
|    |    |-- city: string (nullable = true)
|    |    |-- street: struct (nullable = false)
|    |    |    |-- number: integer (nullable = true)
|    |    |    |-- name: string (nullable = true)

streetNameLens: Lens = Lens(company.address.street.name)
modifiedDF: DataFrame = [name: string, company: struct<name: string, address: struct<city: string, street: struct<number: int, name: string>>>]
res19_3: Employee = Employee(
"john",
Company("awesome inc", Address("london", Street(23, "HIGH STREET")))
)
```

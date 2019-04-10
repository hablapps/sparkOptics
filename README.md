[![Build Status](https://travis-ci.com/hablapps/sparkOptics.svg?token=pvJZNjJ8hxxoMyPVvQ8u&branch=master)](https://travis-ci.com/hablapps/sparkOptics)
![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/oss.sonatype.org/org.hablapps/spark-optics_2.11.svg)

This is the repository for habla computing spark optics for dataframes.
Working with complex structures in spark sql's dataframes can be hard. 
A common case if you are working with complex structures is to modify inner elements of the structure, 
what can be hard to manage.

If we have this structure in a dataframe

```
case class Street(number: Int, name: String)
case class Address(city: String, street: Street)
case class Company(name: String, address: Address)
case class Employee(name: String, company: Company)
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
 
To modify a inner element is hard to do, like changing the name of the street.

```
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
 
This work can be simplified using spark-optics, that allow you to focus in the element that you want to modify,
and the optics will recreate the structure for you.

```
val flashLens = Lens("company.address.street.name")(df.schema)
val modifiedDF = df.select(flashLens.modify(upper):_*)
modifiedDF.printSchema
modifiedDF.as[Employee].head`

`root
|-- name: string (nullable = true)
|-- company: struct (nullable = false)
|    |-- name: string (nullable = true)
|    |-- address: struct (nullable = false)
|    |    |-- city: string (nullable = true)
|    |    |-- street: struct (nullable = false)
|    |    |    |-- number: integer (nullable = true)
|    |    |    |-- name: string (nullable = true)
```
``` 
flashLens: Lens = Lens(company.address.street.name)
modifiedDF: DataFrame = [name: string, company: struct<name: string, address: struct<city: string, street: struct<number: int, name: string>>>]
res19_3: Employee = Employee(
"john",
Company("awesome inc", Address("london", Street(23, "HIGH STREET")))
)
```
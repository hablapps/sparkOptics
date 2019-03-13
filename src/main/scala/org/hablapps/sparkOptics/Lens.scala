package org.hablapps.sparkOptics

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.{DataType, StructType}
import org.hablapps.sparkOptics.Lens.ProtoLens

object Lens {

  object syntax extends LensSyntax

  trait LensSyntax {
    implicit class ProtoLensSyntax(p1: ProtoLens) {
      def combineProtoLens(p2: ProtoLens): ProtoLens = schema => {
        p1(schema) composeProtoLens p2
      }
    }
  }

  type ProtoLens = StructType => Lens

  /**
    * Creates a Lens that focus in the column. The column reference can be provided with dot notation,
    * like in spark "column.subcolum".
    *
    * @param column name of the column to focus
    * @param s      the StructType of the column
    * @return a lens if the struct and the column focused match.
    */
  def apply(column: String)(s: StructType): Lens = {

    def getSonSchema(structType: StructType, name: String): StructType =
      structType.fields
        .find(_.name == name)
        .get
        .dataType
        .asInstanceOf[StructType]

    if (column.contains('.')) {
      val c = column.split("""\.""")
      c.tail
        .foldLeft((createSingle(c.head)(s), s, c.head)) {
          case ((lens, schema, prevTag), colu) =>
            val childSchema = getSonSchema(schema, prevTag)
            (lens composeLens createSingle(colu)(childSchema),
             childSchema,
             colu)
        }
        ._1
    } else {
      createSingle(column)(s)
    }
  }

  private def createSingle(c: String)(s: StructType): Lens = {

    assert(
      s.fields.map(_.name).indexOf(c) >= 0,
      s"the column $c not found in ${s.fields.map(_.name).mkString("[", ",", "]")}")
    new Lens() {
      override def column: Vector[String] = Vector(c)

      /**
        * Schema of the context element.
        *
        * @return an spark [[StructType]]
        */
      override def schema: StructType = s

      override def innerSchema: StructType = s

      /**
        * Sets a new value in the focused lens, with previous columns.
        *
        * @param newValue the new value to set.
        * @param prev     the vector with the strings of the columns that prefix this lens.
        * @return the array of columns with the modifications.
        */
      def setAux(newValue: Column, prev: Vector[String]): Array[Column] = {
        s.fields
          .map(co =>
            if (co.name != c) {
              col((prev :+ co.name).mkString("."))
            } else {
              newValue.as(c)
          })
      }

      /**
        * Renames the column of focused element.
        *
        * @param newName the new name of the column.
        * @param prev    the vector with the strings of the columns that prefix this lens.
        * @return an array of columns with the modifications.
        */
      override def renameWithPrefix(newName: String,
                                    prev: Vector[String]): Array[Column] =
        s.fields
          .map(co =>
            if (co.name != c) {
              col((prev :+ co.name).mkString(".")).as(co.name)
            } else {
              col((prev :+ co.name).mkString(".")).as(newName)
          })

      /**
        * Removes the focused column
        *
        * @param prev the vector with the strings of the columns that prefix this lens.
        * @return an array of columns with the modifications.
        */
      override def prune(prev: Vector[String]): Array[Column] =
        s.fields
          .filter(_.name != c)
          .map(co => col((prev :+ co.name).mkString(".")).as(co.name))

      /**
        * The [[DataType]] of the focused element
        *
        * @return a [[DataType]] value
        */
      override def focusDataType: DataType =
        schema.fields.find(_.name == c).get.dataType
    }
  }
}

sealed abstract class Lens private () {

  /**
    * Focused column vector
    *
    * @return the vector
    */
  def column: Vector[String]

  /**
    * Schema of the context element.
    *
    * @return an spark [[StructType]]
    */
  def schema: StructType

  /**
    * Schema of the structure that holds the focused element.
    *
    * @return an spark [[StructType]]
    */
  def innerSchema: StructType

  /**
    * The [[DataType]] of the focused element
    *
    * @return a [[DataType]] value
    */
  def focusDataType: DataType

  /**
    * The column reference that is focusing.
    *
    * @return an spark [[Column]] reference.
    */
  def get: Column = col(column.mkString("."))

  override def toString: String = column.mkString("Lens(", ".", ")")

  def composeLens(nextLens: Lens): Lens = {
    val first = this
    new Lens {
      override def column: Vector[String] = first.column ++ nextLens.column

      /**
        * Sets a new value in the focused lens, with previous columns.
        *
        * @param newValue the new value to set.
        * @param prev     the vector with the strings of the columns that prefix this lens.
        * @return the array of columns with the modifications.
        */
      override def setAux(newValue: Column,
                          prev: Vector[String]): Array[Column] = {
        val newCol =
          struct(nextLens.setAux(newValue, prev ++ first.column): _*)
            .as(nextLens.column.last)
        first.setAux(newCol, prev)
      }

      /**
        * Schema of the context element.
        *
        * @return an spark [[StructType]]
        */
      override def schema: StructType = first.schema

      /**
        * Schema of the focused element
        *
        * @return an spark [[StructType]]
        */
      override def innerSchema: StructType = nextLens.schema

      /**
        * The [[DataType]] of the focused element
        *
        * @return a [[DataType]] value
        */
      override def focusDataType: DataType = nextLens.focusDataType

      /**
        * Renames the column of focused element.
        *
        * @param newName the new name of the column.
        * @param prev    the vector with the strings of the columns that prefix this lens.
        * @return an array of columns with the modifications.
        */
      override def renameWithPrefix(newName: String,
                                    prev: Vector[String]): Array[Column] = {
        val newCol =
          struct(nextLens.renameWithPrefix(newName, prev ++ first.column): _*)
            .as(nextLens.column.last)
        first.setAux(newCol, prev)
      }

      /**
        * Removes the focused column.
        *
        * @param prev the vector with the strings of the columns that prefix this lens.
        * @return an array of columns with the modifications.
        */
      override def prune(prev: Vector[String]): Array[Column] = {
        val newCol =
          struct(nextLens.prune(prev ++ first.column): _*)
            .as(nextLens.column.last)
        first.setAux(newCol, prev)
      }
    }
  }

  /**
    * Compose a lens with a protoLens.
    *
    * @param nextProto protolens with columns inside the structure already focused.
    * @return a lens focusing into the protoLens reference.
    */
  def composeProtoLens(nextProto: ProtoLens): Lens = {
    val sonStructure = column.foldLeft(schema)((stru, colname) =>
      stru.fields.find(_.name == colname).get.dataType.asInstanceOf[StructType])
    this.composeLens(nextProto(sonStructure))
  }

  /**
    * Sets a new value in the focused lens, with previous columns.
    *
    * @param newValue the new value to set.
    * @param prev     the vector with the strings of the columns that prefix this lens.
    * @return the array of columns with the modifications.
    */
  def setAux(newValue: Column, prev: Vector[String]): Array[Column]

  /**
    * Modifies the column with the provided function.
    *
    * @param f the new value to set.
    * @return the array of columns with the modifications.
    */
  def modify(f: Column => Column): Array[Column] =
    set(f(get))

  /**
    * Sets a value in the focused column.
    *
    * @param c the new value to set.
    * @return the array of columns with the modifications.
    */
  def set(c: Column): Array[Column] =
    setAux(c, Vector.empty)

  /**
    * Renames the column of focused element.
    *
    * @param newName the new name of the column.
    * @return an array with all the columns to use in a select.
    */
  def rename(newName: String): Array[Column] =
    renameWithPrefix(newName, Vector.empty)

  /**
    * Renames the column of focused element.
    *
    * @param newName the new name of the column.
    * @param prev    the vector with the strings of the columns that prefix this lens.
    * @return an array of columns with the modifications.
    */
  def renameWithPrefix(newName: String, prev: Vector[String]): Array[Column]

  /**
    * Removes the focused column
    *
    * @param prev the vector with the strings of the columns that prefix this lens.
    * @return an array of columns with the modifications.
    */
  def prune(prev: Vector[String] = Vector.empty): Array[Column]
}

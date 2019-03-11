package org.hablapps.sparkOptics

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.StructType
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
      override def structure: StructType = s

      def setAux(newValue: Column, prev: Vector[String]): Array[Column] = {
        s.fields
          .map(co =>
            if (co.name != c) {
              col((prev :+ co.name).mkString("."))
            } else {
              newValue.as(c)
          })
      }

      override def renameWithPrefix(newName: String,
                                    prev: Vector[String]): Array[Column] =
        s.fields
          .map(co =>
            if (co.name != c) {
              col((prev :+ co.name).mkString(".")).as(co.name)
            } else {
              col((prev :+ co.name).mkString(".")).as(newName)
            })

      override def prune(prev: Vector[String]): Array[Column] =
        s.fields
          .filter(_.name != c)
          .map(co => col((prev :+ co.name).mkString(".")).as(co.name))
    }
  }
}

sealed abstract class Lens private () {

  def column: Vector[String]
  def structure: StructType

  def get: Column = col(column.mkString("."))

  override def toString: String = "Lens(" + column + ")"

  def composeLens(nextLens: Lens): Lens = {
    val first = this
    new Lens {
      override def column: Vector[String] = first.column ++ nextLens.column

      override def setAux(newValue: Column,
                          prev: Vector[String]): Array[Column] = {
        val newCol =
          struct(nextLens.setAux(newValue, prev ++ first.column): _*)
            .as(nextLens.column.last)
        first.setAux(newCol, prev)
      }

      override def structure: StructType = first.structure

      override def renameWithPrefix(newName: String,
                                    prev: Vector[String]): Array[Column] = {
        val newCol =
          struct(nextLens.renameWithPrefix(newName, prev ++ first.column): _*)
            .as(nextLens.column.last)
        first.setAux(newCol, prev)
      }

      override def prune(prev: Vector[String]): Array[Column] = {
        val newCol =
          struct(nextLens.prune(prev ++ first.column): _*)
            .as(nextLens.column.last)
        first.setAux(newCol, prev)
      }
    }
  }

  def composeProtoLens(nextProto: ProtoLens): Lens = {
    val sonStructure = column.foldLeft(structure)((stru, colname) =>
      stru.fields.find(_.name == colname).get.dataType.asInstanceOf[StructType])
    this.composeLens(nextProto(sonStructure))
  }

  def setAux(newValue: Column, prev: Vector[String]): Array[Column]

  def cleanColumns: Array[Column] = structure.fields.map(f => col(f.name))

  def modify(f: Column => Column): Array[Column] =
    set(f(get))

  def set(c: Column): Array[Column] =
    setAux(c, Vector.empty)

  def rename(newName: String): Array[Column] =
    renameWithPrefix(newName, Vector.empty)

  def renameWithPrefix(newName: String, prev: Vector[String]): Array[Column]

  def prune(prev: Vector[String] = Vector.empty): Array[Column]
}

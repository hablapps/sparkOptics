package org.hablapps.sparkOptics

import org.apache.spark.sql.{Column, DataFrame}
import org.hablapps.sparkOptics.ProtoLens.ProtoLens

object GlassesFrame {

  def withLens(lens: Lens): GlassesFrame[Lens] =
    new GlassesFrame[Lens] {
      override def set(newValue: Column): DataFrame => DataFrame = {
        _.select(lens.set(newValue): _*)
      }

      override def get: DataFrame => Column = _ => lens.get

      override def modify(f: Column => Column): DataFrame => DataFrame =
        _.select(lens.modify(f): _*)

      override val getOptic: Lens = lens
    }

  def withProtoLens(protoLens: ProtoLens): GlassesFrame[ProtoLens] =
    new GlassesFrame[ProtoLens] {
      override def set(newValue: Column): DataFrame => DataFrame =
        df => df.select(protoLens(df.schema).set(newValue): _*)

      override def get: DataFrame => Column = df => protoLens(df.schema).get

      override def modify(f: Column => Column): DataFrame => DataFrame =
        df => df.select(protoLens(df.schema).modify(f): _*)

      override val getOptic: ProtoLens = protoLens
    }

  object syntax extends GlassesFrameSyntax

  trait GlassesFrameSyntax {
    implicit class GlassesProtoLensSyntax(protoLens: ProtoLens) {
      def onGlasses: GlassesFrame[ProtoLens] =
        withProtoLens(protoLens)
    }
    implicit class GlassesLensSyntax(lens: Lens) {
      def onGlasses: GlassesFrame[Lens] =
        withLens(lens)
    }
  }
}

abstract sealed class GlassesFrame[A]() {

  def getOptic: A

  def set(newValue: Column): DataFrame => DataFrame

  def get: DataFrame => Column

  def modify(f: Column => Column): DataFrame => DataFrame

  def setAndCheckSchema(newValue: Column): DataFrame => DataFrame = df => {
    val newDf = set(newValue)(df)
    if (df.schema == newDf.schema) {
      newDf
    } else {
      throw new Exception(
        "The returned schema is not the same as the original one")
    }
  }

  def modifyAndCheckSchema(f: Column => Column): DataFrame => DataFrame =
    df => {
      val newDf = modify(f)(df)
      if (df.schema == newDf.schema) {
        newDf
      } else {
        throw new Exception(
          "The returned schema is not the same as the original one")
      }
    }

}

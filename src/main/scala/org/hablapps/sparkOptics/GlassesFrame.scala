package org.hablapps.sparkOptics

import org.apache.spark.sql.{Column, DataFrame}
import org.hablapps.sparkOptics.ProtoLens.ProtoLens

object GlassesFrame {

  object syntax extends GlassesFrameSyntax with GlassesFrameInstances

  trait GlassesFrameSyntax {

    implicit class GlassesProtoLensSyntax[O](optic: O)(
        implicit glasses: GlassesFrame[O]) {
      def setDF(newValue: Column): DataFrame => DataFrame =
        glasses.set(optic)(newValue)

      def setDFCheckingSchema(newValue: Column): DataFrame => DataFrame =
        glasses.setAndCheckSchema(optic)(newValue)

      def getDF: DataFrame => Column =
        glasses.get(optic)

      def modifyDF(f: Column => Column): DataFrame => DataFrame =
        glasses.modify(optic)(f)

      def modifyDFCheckingSchema(f: Column => Column): DataFrame => DataFrame =
        glasses.modifyAndCheckSchema(optic)(f)
    }

  }

  trait GlassesFrameInstances {
    implicit val lensGlasses: GlassesFrame[Lens] =
      new GlassesFrame[Lens] {
        override def set(optic: Lens)(
            newValue: Column): DataFrame => DataFrame = {
          _.select(optic.set(newValue): _*)
        }

        override def get(optic: Lens): DataFrame => Column = _ => optic.get

        override def modify(optic: Lens)(
            f: Column => Column): DataFrame => DataFrame =
          _.select(optic.modify(f): _*)
      }

    implicit val protoLensGlasses: GlassesFrame[ProtoLens] =
      new GlassesFrame[ProtoLens] {
        override def set(optic: ProtoLens)(
            newValue: Column): DataFrame => DataFrame =
          df => lensGlasses.set(optic(df.schema))(newValue)(df)

        override def get(optic: ProtoLens): DataFrame => Column =
          df => lensGlasses.get(optic(df.schema))(df)

        override def modify(optic: ProtoLens)(
            f: Column => Column): DataFrame => DataFrame =
          df => lensGlasses.modify(optic(df.schema))(f)(df)

      }
  }

}

trait GlassesFrame[A] {

  def set(optic: A)(newValue: Column): DataFrame => DataFrame

  def get(optic: A): DataFrame => Column

  def modify(optic: A)(f: Column => Column): DataFrame => DataFrame

  def setAndCheckSchema(optic: A)(newValue: Column): DataFrame => DataFrame =
    df => {
      val newDf = set(optic)(newValue)(df)
      if (df.schema == newDf.schema) {
        newDf
      } else {
        throw new Exception(
          "The returned schema is not the same as the original one")
      }
    }

  def modifyAndCheckSchema(optic: A)(
      f: Column => Column): DataFrame => DataFrame =
    df => {
      val newDf = modify(optic)(f)(df)
      if (df.schema == newDf.schema) {
        newDf
      } else {
        throw new Exception(
          "The returned schema is not the same as the original one")
      }
    }

}

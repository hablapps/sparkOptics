package org.hablapps.sparkOptics

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.StructType
import org.hablapps.sparkOptics.DFTraversal.ProtoDFTraversal
import org.hablapps.sparkOptics.Lens.ProtoLens

object ProtoDFTraversal {
  def apply: ProtoDFTraversal = DFTraversal(_)

  object syntax extends ProtoDFTraversalSyntax {}

  trait ProtoDFTraversalSyntax {

    implicit class ProtoDFTraversalSyntax(proto: ProtoDFTraversal) {
      def composeLens(lens: Lens): DFTraversal = {
        proto(lens.schema) composeLens lens
      }

      def composeProtoLens(protoLens: ProtoLens): ProtoDFTraversal =
        structure => {
          proto(structure) composeProtoLens protoLens
        }

      def apply(df: DataFrame): DFTraversal = {
        proto(df.schema)
      }
    }
  }
}

object DFTraversal {

  type ProtoDFTraversal = StructType => DFTraversal

  def apply(struct: StructType): DFTraversal = {
    new DFTraversal {
      override def schema: StructType = struct

      override def opticsToApply: Option[Lens] = None

      override def set(newValue: Column): DataFrame => DataFrame = {
        if (schema.fields.length == 1) {
          _.withColumn(schema.fields.head.name, newValue)
        } else {
          _.select(newValue)
        }
      }

      override def get: DataFrame => DataFrame = x => x
    }
  }
}

abstract sealed class DFTraversal() {
  def schema: StructType

  def set(newValue: Column): DataFrame => DataFrame

  def get: DataFrame => DataFrame

  def opticsToApply: Option[Lens]

  def composeLens(lens: Lens): DFTraversal = {
    val traversalParent = this
    new DFTraversal {
      override def schema: StructType = traversalParent.schema

      override def set(newValue: Column): DataFrame => DataFrame =
        _.select(opticsToApply.get.set(newValue): _*)

      override def opticsToApply: Option[Lens] =
        Some(traversalParent.opticsToApply.fold(lens)(_ composeLens lens))

      override def get: DataFrame => DataFrame = _.select(opticsToApply.get.get)
    }
  }

  def composeProtoLens(protoLens: ProtoLens): DFTraversal = {
    val traversalParent = this
    new DFTraversal {
      override def schema: StructType = traversalParent.schema

      override def set(newValue: Column): DataFrame => DataFrame =
        _.select(opticsToApply.get.set(newValue): _*)

      override def opticsToApply: Option[Lens] =
        Some(
          traversalParent.opticsToApply.fold(protoLens(traversalParent.schema))(
            _ composeProtoLens protoLens))

      override def get: DataFrame => DataFrame = _.select(opticsToApply.get.get)
    }
  }

}

package org.hablapps.sparkOptics

import org.apache.spark.sql.types.StructType

object ProtoLens {
  type ProtoLens = StructType => Lens
  def apply(column: String): ProtoLens = Lens(column)

  object syntax extends ProtoLensSyntax

  trait ProtoLensSyntax {
    implicit class ProtoLensSyntax(p1: ProtoLens) {
      def composeProtoLens(p2: ProtoLens): ProtoLens = schema => {
        p1(schema) composeProtoLens p2
      }
    }

    type ProtoLens = StructType => Lens
  }
}

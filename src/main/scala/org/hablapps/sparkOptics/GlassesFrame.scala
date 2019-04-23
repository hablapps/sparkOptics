package org.hablapps.sparkOptics

import org.apache.spark.sql.types.{DataType, StructField}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.hablapps.sparkOptics.ProtoLens.ProtoLens

object GlassesFrame {

  object syntax extends GlassesFrameSyntax with GlassesFrameInstances

  trait GlassesFrameSyntax {

    implicit class GlassesProtoLensSyntax[O](optic: O)(
        implicit glasses: GlassesFrame[O]) {

      /**
        * Sets the focused element to the provided one
        * @param newValue the
        * @return a function that transforms a Dataset to a DataFrame
        * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset org.apache.spark.sql.Dataset]]
        * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column org.apache.spark.sql.Column]]
        */
      def setDF(newValue: Column): Dataset[_] => DataFrame =
        glasses.set(optic)(newValue)

      /**
        * Sets the focused element to the provided one, and throws an error if the schema of the returned DataFrame is not the same as the provided one.
        * @param newValue the value to be setted in
        * @return a function that transforms a Dataset to a DataFrame
        * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset org.apache.spark.sql.Dataset]]
        * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column org.apache.spark.sql.Column]]
        * @throws SchemaChangedException the error showing the differences in the schema
        */
      def setDFCheckingSchema(newValue: Column): Dataset[_] => DataFrame =
        glasses.setAndCheckSchema(optic)(newValue)

      /**
        * Returns a function that extract the column of a dataset
        * @return a function that transforms a Dataset to the focused column
        * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset org.apache.spark.sql.Dataset]]
        * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column org.apache.spark.sql.Column]]
        */
      def getDF: Dataset[_] => Column =
        glasses.get(optic)

      /**
        * Sets the focused element to the provided one
        * @param f the function to apply to the focused column
        * @return a function that transforms a Dataset to a DataFrame
        * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset org.apache.spark.sql.Dataset]]
        * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column org.apache.spark.sql.Column]]
        */
      def modifyDF(f: Column => Column): Dataset[_] => DataFrame =
        glasses.modify(optic)(f)

      /**
        * Modifies the focused element to the provided one, and throws an error if the schema of the returned DataFrame is not the same as the provided one.
        * @param f the function to apply to the focused column
        * @return a function that transforms a Dataset to a DataFrame
        * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset org.apache.spark.sql.Dataset]]
        * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column org.apache.spark.sql.Column]]
        * @throws SchemaChangedException the error showing the differences in the schema
        */
      def modifyDFCheckingSchema(f: Column => Column): Dataset[_] => DataFrame =
        glasses.modifyAndCheckSchema(optic)(f)
    }

  }

  trait GlassesFrameInstances {
    implicit val lensGlasses: GlassesFrame[Lens] =
      new GlassesFrame[Lens] {

        override def set(optic: Lens)(
            newValue: Column): Dataset[_] => DataFrame = {
          _.select(optic.set(newValue): _*)
        }

        override def get(optic: Lens): Dataset[_] => Column = _ => optic.get

        override def modify(optic: Lens)(
            f: Column => Column): Dataset[_] => DataFrame =
          _.select(optic.modify(f): _*)
      }

    implicit val protoLensGlasses: GlassesFrame[ProtoLens] =
      new GlassesFrame[ProtoLens] {
        override def set(optic: ProtoLens)(
            newValue: Column): Dataset[_] => DataFrame =
          df => lensGlasses.set(optic(df.schema))(newValue)(df)

        override def get(optic: ProtoLens): Dataset[_] => Column =
          df => lensGlasses.get(optic(df.schema))(df)

        override def modify(optic: ProtoLens)(
            f: Column => Column): Dataset[_] => DataFrame =
          df => lensGlasses.modify(optic(df.schema))(f)(df)

      }
  }

}

trait GlassesFrame[A] {

  private def modifiedFieldIsEqualType(
      df1: Dataset[_],
      df2: Dataset[_],
      optic: A): Either[SchemaChangedException, Unit] = {
    compareFields(df1.select(get(optic)(df1)).schema.fields.head,
                  df2.select(get(optic)(df2)).schema.fields.head,
                  optic)
  }

  private def compareFields(a: StructField,
                            b: StructField,
                            optic: A): Either[SchemaChangedException, Unit] =
    for {
      _ <- Either
        .cond(
          a.dataType.getClass.getCanonicalName == b.dataType.getClass.getCanonicalName,
          (),
          TypeChangedException(a.dataType, b.dataType, optic)
        )
        .right
      _ <- Either
        .cond(
          a.nullable == b.nullable,
          (),
          NullableChangedException(a.nullable, b.nullable, optic)
        )
        .right
    } yield ()

  /**
    * Sets the focused element to the provided one.
    * @param optic the optic to apply
    * @param newValue the
    * @return a function that transforms a Dataset to a DataFrame
    * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset org.apache.spark.sql.Dataset]]
    * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column org.apache.spark.sql.Column]]
    */
  def set(optic: A)(newValue: Column): Dataset[_] => DataFrame

  /**
    * Returns a function that extract the column of a dataset.
    * @param optic the optic to apply
    * @return a function that transforms a Dataset to the focused column
    * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset org.apache.spark.sql.Dataset]]
    * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column org.apache.spark.sql.Column]]
    */
  def get(optic: A): Dataset[_] => Column

  /**
    * Sets the focused element to the provided one.
    * @param optic the optic to apply
    * @param f the function to apply to the focused column
    * @return a function that transforms a Dataset to a DataFrame
    * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset org.apache.spark.sql.Dataset]]
    */
  def modify(optic: A)(f: Column => Column): Dataset[_] => DataFrame

  /**
    * Sets the focused element to the provided one, and throws an error if the schema of the returned DataFrame is not the same as the provided one.
    * @param optic the optic to apply
    * @param newValue the value to be setted in
    * @return a function that transforms a Dataset to a DataFrame
    * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset org.apache.spark.sql.Dataset]]
    * @throws SchemaChangedException the error showing the differences in the schema
    */
  def setAndCheckSchema(optic: A)(newValue: Column): Dataset[_] => DataFrame =
    df => {
      val newDf = set(optic)(newValue)(df)
      val validation = modifiedFieldIsEqualType(df, newDf, optic)
      if (validation.isRight) {
        newDf
      } else {
        throw validation.left.get
      }
    }

  /**
    * Modifies the focused element to the provided one, and throws an error if the schema of the returned DataFrame is not the same as the provided one.
    * @param optic the optic to apply
    * @param f the function to apply to the focused column
    * @return a function that transforms a Dataset to a DataFrame
    * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset org.apache.spark.sql.Dataset]]
    * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column org.apache.spark.sql.Column]]
    * @throws SchemaChangedException the error showing the differences in the schema
    */
  def modifyAndCheckSchema(optic: A)(
      f: Column => Column): Dataset[_] => DataFrame =
    df => {
      val newDf = modify(optic)(f)(df)
      val validation = modifiedFieldIsEqualType(df, newDf, optic)
      if (validation.isRight) {
        newDf
      } else {
        throw validation.left.get
      }
    }

}

/**
  * Represents an error in the comparation of two spark schemas.
  * @param message the error message
  */
abstract class SchemaChangedException(message: String)
    extends Exception(message)
case class NullableChangedException[O](previousNull: Boolean,
                                       newNull: Boolean,
                                       optic: O)
    extends SchemaChangedException(
      s"The original column nullable that was $previousNull changed to $newNull using ${optic.toString}"
    )
case class TypeChangedException[O](previousType: DataType,
                                   newType: DataType,
                                   optic: O)
    extends SchemaChangedException(
      s"The original column type that was ${previousType.getClass.getSimpleName
        .filter(_ != '$')} " +
        s"changed to ${newType.getClass.getSimpleName.filter(_ != '$')} " +
        s"using ${optic.toString}")

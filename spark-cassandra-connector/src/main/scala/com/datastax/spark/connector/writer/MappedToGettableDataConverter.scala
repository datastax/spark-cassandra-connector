package com.datastax.spark.connector.writer

import scala.language.existentials
import scala.reflect.runtime.universe._
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.ReflectionLock.SparkReflectionLock

import com.datastax.spark.connector.util.{Symbols, ReflectionUtil}
import com.datastax.spark.connector.{GettableByIndexData, TupleValue, UDTValue, ColumnRef}
import com.datastax.spark.connector.cql.StructDef
import com.datastax.spark.connector.mapper._
import com.datastax.spark.connector.types.{TupleType, MapType, SetType, ListType, ColumnType, TypeConverter}

private[connector] object MappedToGettableDataConverter {

  def apply[T : TypeTag : ColumnMapper]
      (struct: StructDef, columnSelection: IndexedSeq[ColumnRef]): TypeConverter[struct.ValueRepr] =

    new TypeConverter[struct.ValueRepr] {

      /** To determine which default column mapper to use for UDT fields.
        * If this class is a Java bean, then the UDT fields will be mapped with
        * the `JavaBeanColumnMapper` */
      private val isJavaBean =
        implicitly[ColumnMapper[T]].isInstanceOf[JavaBeanColumnMapper[_]]

      private val columnMap =
        implicitly[ColumnMapper[T]].columnMapForWriting(struct, columnSelection)

      /** Returns the column mapper associated with the given type.
        * Used to find column mappers for UDT columns.
        * For tuples, returns [[com.datastax.spark.connector.mapper.TupleColumnMapper TupleColumnMapper]],
        * for Java beans uses
        * [[com.datastax.spark.connector.mapper.JavaBeanColumnMapper JavaBeanColumnMapper]],
        * and for everything else uses
        * [[com.datastax.spark.connector.mapper.DefaultColumnMapper DefaultColumnMapper]] */
      private def columnMapper[U: TypeTag]: ColumnMapper[U] = {
        val tpe = SparkReflectionLock.synchronized(typeTag[U].tpe)
        if (ReflectionUtil.isScalaTuple(tpe))
          new TupleColumnMapper[U]
        else if (isJavaBean)
          new JavaBeanColumnMapper[U]()(ReflectionUtil.classTag[U])
        else
          new DefaultColumnMapper[U]
      }

      /** Returns true for tuple types that provide full type information of their components */
      private def isTypedTuple(sym: Symbol): Boolean =
        ReflectionUtil.isScalaTuple(sym) ||
          sym == Symbols.PairSymbol ||
          sym == Symbols.TripleSymbol

      /** Returns a converter for converting given column to appropriate type savable to Cassandra */
      private def converter[U : TypeTag](columnType: ColumnType[_]): TypeConverter[_ <: AnyRef] = {
        SparkReflectionLock.synchronized {
          val scalaType = typeTag[U].tpe

          (columnType, scalaType) match {
            // Collections need recursive call to get the converter for the collection elements
            // to handle nested UDT values and tuples properly.
            case (ListType(argColumnType), TypeRef(_, _, List(argScalaType))) =>
              val argConverter = converter(argColumnType, argScalaType)
              TypeConverter.javaArrayListConverter(argConverter)

            case (SetType(argColumnType), TypeRef(_, _, List(argScalaType))) =>
              val argConverter = converter(argColumnType, argScalaType)
              TypeConverter.javaHashSetConverter(argConverter)

            case (MapType(keyColumnType, valueColumnType),
                  TypeRef(_, _, List(keyScalaType, valueScalaType))) =>
              val keyConverter = converter(keyColumnType, keyScalaType)
              val valueConverter = converter(valueColumnType, valueScalaType)
              TypeConverter.javaHashMapConverter(keyConverter, valueConverter)

            case (tt @ TupleType(argColumnType1, argColumnType2),
                  TypeRef(_, Symbols.PairSymbol, List(argScalaType1, argScalaType2))) =>
              val c1 = converter(argColumnType1.columnType, argScalaType1)
              val c2 = converter(argColumnType2.columnType, argScalaType2)
              tt.converterToCassandra(IndexedSeq(c1, c2))

            case (tt @ TupleType(argColumnType1, argColumnType2, argColumnType3),
                  TypeRef(_, Symbols.TripleSymbol, List(argScalaType1, argScalaType2, argScalaType3))) =>
              val c1 = converter(argColumnType1.columnType, argScalaType1)
              val c2 = converter(argColumnType2.columnType, argScalaType2)
              val c3 = converter(argColumnType3.columnType, argScalaType3)
              tt.converterToCassandra(IndexedSeq(c1, c2, c3))

            // If tuple with mismatched number of components, don't continue:
            case (tt: TupleType, TypeRef(_, symbol, args))
                if isTypedTuple(symbol) && tt.columns.length != args.length =>
              throw new IllegalArgumentException(
                s"Expected ${tt.columns.length} components in the tuple, " +
                  s"instead of ${args.length} in $scalaType")

            // UDTValue, TupleValue:
            case (t: StructDef, _) if scalaType <:< typeOf[GettableByIndexData] =>
              columnType.converterToCassandra

            //Options
            case (t: StructDef, TypeRef(_, _, List(argScalaType))) if scalaType <:< typeOf[Option[Any]] =>
              type U2 = u2 forSome {type u2}
              implicit val tt = ReflectionUtil.typeToTypeTag[U2](argScalaType)
              implicit val cm: ColumnMapper[U2] = columnMapper[U2]
              apply[U2](t, t.columnRefs)

            // UDTs mapped to case classes and tuples mapped to Scala tuples.
            // ColumnMappers support mapping Scala tuples, so we don't need a special case for them.
            case (t: StructDef, _) =>
              implicit val cm: ColumnMapper[U] = columnMapper[U]
              apply[U](t, t.columnRefs)

            // Primitive types
            case _ =>
              columnType.converterToCassandra
          }
        }
      }

      /** Returns a converter that can convert
        * a Scala type given by `Type` to a Cassandra value of given `ColumnType` */
      private def converter(columnType: ColumnType[_], tpe: Type): TypeConverter[_ <: AnyRef] = {
        type U = u forSome {type u}
        implicit val tt = ReflectionUtil.typeToTypeTag[U](tpe)
        converter[U](columnType)
      }

      @transient
      private val tpe = SparkReflectionLock.synchronized {
        typeTag[T].tpe
      }

      private val cls = typeTag[T].mirror.runtimeClass(typeTag[T].tpe).asInstanceOf[Class[T]]
      private val typeName = tpe.toString

      val columnNames =
        columnSelection.map(_.columnName)

      private val getterByColumnName = columnMap.getters.map {
        case (name, colRef) => (colRef.columnName, name)
      }

      private val getters =
        columnNames.map(getterByColumnName)

      @transient
      private val scalaTypes: IndexedSeq[Type] =
        getters.map(ReflectionUtil.returnType(tpe, _))

      private val columnTypes: IndexedSeq[ColumnType[_]] =
        columnNames.map(c => struct.columnByName(c).columnType)

      private val extractor =
        new PropertyExtractor(cls, getters)

      private val converters = {
        for (i <- columnNames.indices) yield {
          try {
            val ct = columnTypes(i)
            val st = scalaTypes(i)
            converter(ct, st)
          } catch {
            case NonFatal(e) =>
              throw new IllegalArgumentException(
                s"""Failed to get converter for field "${getters(i)}"
                   |of type ${scalaTypes(i)} in $typeName
                   |mapped to column "${columnNames(i)}" of "${struct.name}"
                   |""".stripMargin.replaceAll("\n", " "), e)
          }
        }
      }

      override def targetTypeTag = typeTag[struct.ValueRepr]

      override def convertPF = {
        case obj if cls.isInstance(obj) =>
          val columnValues = extractor.extract(obj.asInstanceOf[T])
          for (i <- columnValues.indices)
            columnValues(i) = converters(i).convert(columnValues(i))
          struct.newInstance(columnValues: _*)
        case Some(obj) if cls.isInstance(obj) =>
          val columnValues = extractor.extract(obj.asInstanceOf[T])
          for (i <- columnValues.indices)
            columnValues(i) = converters(i).convert(columnValues(i))
          struct.newInstance(columnValues: _*)
        case None =>
          null.asInstanceOf[struct.ValueRepr]
      }
    }
}

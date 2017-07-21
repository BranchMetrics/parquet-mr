/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Log;
import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.IncompatibleSchemaModificationException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link WriteSupport} for writing Protocol Buffers.
 * @author Lukas Nalezenec
 */
public class ProtoWriteSupport<T extends MessageOrBuilder> extends WriteSupport<T> {

  private static final Log LOG = Log.getLog(ProtoWriteSupport.class);
  public static final String PB_CLASS_WRITE = "parquet.proto.writeClass";

  public static final String WRITE_OLD_LIST_STRUCTURE =
          "parquet.avro.write-old-list-structure";
  static final boolean WRITE_OLD_LIST_STRUCTURE_DEFAULT = false;

  private static final String LIST_REPEATED_NAME = "list";
  public static final String OLD_LIST_REPEATED_NAME = "array";
  public static final String LIST_ELEMENT_NAME = "element";

  private RecordConsumer recordConsumer;
  private Class<? extends Message> protoMessage;
  private ListWriter listWriter;

  private MessageType rootSchema;
  private Descriptors.Descriptor rootMessageDescriptor;

  public ProtoWriteSupport() {
  }

  public ProtoWriteSupport(Class<? extends Message> protobufClass) {
    this.protoMessage = protobufClass;
  }

  public static void setSchema(Configuration configuration, Class<? extends Message> protoClass) {
    configuration.setClass(PB_CLASS_WRITE, protoClass, Message.class);
  }

  /**
   * Writes Protocol buffer to parquet file.
   * @param record instance of Message.Builder or Message.
   * */
  @Override
  public void write(T record) {
    recordConsumer.startMessage();
    try {
      writeRecordFields(rootSchema, rootMessageDescriptor, (Message) record);
    } catch (RuntimeException e) {
      Message m = (record instanceof Message.Builder) ? ((Message.Builder) record).build() : (Message) record;
      LOG.error("Cannot write message " + e.getMessage() + " : " + m);
      throw e;
    }
    recordConsumer.endMessage();
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public WriteContext init(Configuration configuration) {

    // if no protobuf descriptor was given in constructor, load descriptor from configuration (set with setProtobufClass)
    if (protoMessage == null) {
      Class<? extends Message> pbClass = configuration.getClass(PB_CLASS_WRITE, null, Message.class);
      if (pbClass != null) {
        protoMessage = pbClass;
      } else {
        String msg = "Protocol buffer class not specified.";
        String hint = " Please use method ProtoParquetOutputFormat.setProtobufClass(...) or other similar method.";
        throw new BadConfigurationException(msg + hint);
      }
    }

    rootSchema = new ProtoSchemaConverterV2().convert(protoMessage);
    rootMessageDescriptor = Protobufs.getMessageDescriptor(protoMessage);
//    validatedMapping(messageDescriptor, rootSchema);

//    this.messageWriter = new MessageWriter(messageDescriptor, rootSchema);
    boolean writeOldListStructure = configuration.getBoolean(
            WRITE_OLD_LIST_STRUCTURE, WRITE_OLD_LIST_STRUCTURE_DEFAULT);
    if (writeOldListStructure) {
      this.listWriter = new TwoLevelListWriter();
    } else {
      this.listWriter = new ThreeLevelListWriter();
    }

    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(ProtoReadSupport.PB_CLASS, protoMessage.getName());
    extraMetaData.put(ProtoReadSupport.PB_DESCRIPTOR, serializeDescriptor(protoMessage));
    return new WriteContext(rootSchema, extraMetaData);
  }


  class FieldWriter {
    String fieldName;
    int index = -1;

     void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }

    /** sets index of field inside parquet message.*/
     void setIndex(int index) {
      this.index = index;
    }

    /** Used for writing repeated fields*/
     void writeRawValue(Object value) {

    }

    /** Used for writing nonrepeated (optional, required) fields*/
    void writeField(Object value) {
      recordConsumer.startField(fieldName, index);
      writeRawValue(value);
      recordConsumer.endField(fieldName, index);
    }
  }

  class MessageWriter extends FieldWriter {

    final FieldWriter[] fieldWriters;

    @SuppressWarnings("unchecked")
    MessageWriter(Descriptors.Descriptor descriptor, GroupType schema) {
      List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
      fieldWriters = (FieldWriter[]) Array.newInstance(FieldWriter.class, fields.size());

      int i = 0;
      for (Descriptors.FieldDescriptor fieldDescriptor: fields) {
        String name = fieldDescriptor.getName();
        Type type = schema.getType(name);
        FieldWriter writer = createWriter(fieldDescriptor, type);

        if(fieldDescriptor.isRepeated()) {
         writer = new ArrayWriter(writer);
        }

        writer.setFieldName(name);
        writer.setIndex(schema.getFieldIndex(name));

        fieldWriters[i] = writer;
        i++;
      }
    }

    private FieldWriter createWriter(Descriptors.FieldDescriptor fieldDescriptor, Type type) {

      switch (fieldDescriptor.getJavaType()) {
        case STRING: return new StringWriter() ;
        case MESSAGE: return new MessageWriter(fieldDescriptor.getMessageType(), type.asGroupType());
        case INT: return new IntWriter();
        case LONG: return new LongWriter();
        case FLOAT: return new FloatWriter();
        case DOUBLE: return new DoubleWriter();
        case ENUM: return new EnumWriter();
        case BOOLEAN: return new BooleanWriter();
        case BYTE_STRING: return new BinaryWriter();
      }

      return unknownType(fieldDescriptor);//should not be executed, always throws exception.
    }

    /** Writes top level message. It cannot call startGroup() */
    void writeTopLevelMessage(Object value) {
      writeAllFields((MessageOrBuilder) value);
    }

    /** Writes message as part of repeated field. It cannot start field*/
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.startGroup();
      writeAllFields((MessageOrBuilder) value);
      recordConsumer.endGroup();
    }

    /** Used for writing nonrepeated (optional, required) fields*/
    @Override
    final void writeField(Object value) {
      recordConsumer.startField(fieldName, index);
      recordConsumer.startGroup();
      writeAllFields((MessageOrBuilder) value);
      recordConsumer.endGroup();
      recordConsumer.endField(fieldName, index);
    }

    private void writeAllFields(MessageOrBuilder pb) {
      //returns changed fields with values. Map is ordered by id.
      Map<Descriptors.FieldDescriptor, Object> changedPbFields = pb.getAllFields();

      for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : changedPbFields.entrySet()) {
        Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
        int fieldIndex = fieldDescriptor.getIndex();
        fieldWriters[fieldIndex].writeField(entry.getValue());
      }
    }
  }

  class ArrayWriter extends FieldWriter {
    final FieldWriter fieldWriter;

    ArrayWriter(FieldWriter fieldWriter) {
      this.fieldWriter = fieldWriter;
    }

    @Override
    final void writeRawValue(Object value) {
      throw new UnsupportedOperationException("Array has no raw value");
    }

    @Override
    final void writeField(Object value) {
      recordConsumer.startField(fieldName, index);
      List<?> list = (List<?>) value;

      for (Object listEntry: list) {
        fieldWriter.writeRawValue(listEntry);
      }

      recordConsumer.endField(fieldName, index);
    }
  }

  /** validates mapping between protobuffer fields and parquet fields.*/
  private void validatedMapping(Descriptors.Descriptor descriptor, GroupType parquetSchema) {
    List<Descriptors.FieldDescriptor> allFields = descriptor.getFields();

    for (Descriptors.FieldDescriptor fieldDescriptor: allFields) {
      String fieldName = fieldDescriptor.getName();
      int fieldIndex = fieldDescriptor.getIndex();
      int parquetIndex = parquetSchema.getFieldIndex(fieldName);
      if (fieldIndex != parquetIndex) {
        String message = "FieldIndex mismatch name=" + fieldName + ": " + fieldIndex + " != " + parquetIndex;
        throw new IncompatibleSchemaModificationException(message);
      }
    }
  }


  class StringWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      Binary binaryString = Binary.fromString((String) value);
      recordConsumer.addBinary(binaryString);
    }
  }

  class IntWriter extends FieldWriter {
  @Override
    final void writeRawValue(Object value) {
      recordConsumer.addInteger((Integer) value);
    }
  }

  class LongWriter extends FieldWriter {

    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addLong((Long) value);
    }
  }

  class FloatWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addFloat((Float) value);
    }
  }

  class DoubleWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addDouble((Double) value);
    }
  }

  class EnumWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      Binary binary = Binary.fromString(((Descriptors.EnumValueDescriptor) value).getName());
      recordConsumer.addBinary(binary);
    }
  }

  class BooleanWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addBoolean((Boolean) value);
    }
  }

  class BinaryWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      ByteString byteString = (ByteString) value;
      Binary binary = Binary.fromConstantByteArray(byteString.toByteArray());
      recordConsumer.addBinary(binary);
    }
  }

  private FieldWriter unknownType(Descriptors.FieldDescriptor fieldDescriptor) {
    String exceptionMsg = "Unknown type with descriptor \"" + fieldDescriptor
            + "\" and type \"" + fieldDescriptor.getJavaType() + "\".";
    throw new InvalidRecordException(exceptionMsg);
  }

  /** Returns message descriptor as JSON String*/
  private String serializeDescriptor(Class<? extends Message> protoClass) {
    Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(protoClass);
    DescriptorProtos.DescriptorProto asProto = descriptor.toProto();
    return TextFormat.printToString(asProto);
  }

  private abstract class ListWriter {

    protected abstract void writeCollection(
            GroupType type, Descriptors.FieldDescriptor descriptor, Collection<?> collection);

    protected abstract void writeObjectArray(
            GroupType type, Descriptors.FieldDescriptor descriptor, Object[] array);

    protected abstract void startArray();

    protected abstract void endArray();

    public void writeList(GroupType schema, Descriptors.FieldDescriptor descriptor, Object value) {
      recordConsumer.startGroup(); // group wrapper (original type LIST)
      if (value instanceof Collection) {
        writeCollection(schema, descriptor, (Collection) value);
      } else {
        Class<?> arrayClass = value.getClass();
        Preconditions.checkArgument(arrayClass.isArray(),
                "Cannot write unless collection or array: " + arrayClass.getName());
        writeJavaArray(schema, descriptor, arrayClass, value);
      }
      recordConsumer.endGroup();
    }

    public void writeJavaArray(GroupType schema, Descriptors.FieldDescriptor descriptor,
                               Class<?> arrayClass, Object value) {
      Class<?> elementClass = arrayClass.getComponentType();

      if (!elementClass.isPrimitive()) {
        writeObjectArray(schema, descriptor, (Object[]) value);
        return;
      }

      switch (descriptor.getJavaType()) {
        case BOOLEAN:
          Preconditions.checkArgument(elementClass == boolean.class,
                  "Cannot write as boolean array: " + arrayClass.getName());
          writeBooleanArray((boolean[]) value);
          break;
        case INT:
          if (elementClass == byte.class) {
            writeByteArray((byte[]) value);
          } else if (elementClass == char.class) {
            writeCharArray((char[]) value);
          } else if (elementClass == short.class) {
            writeShortArray((short[]) value);
          } else if (elementClass == int.class) {
            writeIntArray((int[]) value);
          } else {
            throw new IllegalArgumentException(
                    "Cannot write as an int array: " + arrayClass.getName());
          }
          break;
        case LONG:
          Preconditions.checkArgument(elementClass == long.class,
                  "Cannot write as long array: " + arrayClass.getName());
          writeLongArray((long[]) value);
          break;
        case FLOAT:
          Preconditions.checkArgument(elementClass == float.class,
                  "Cannot write as float array: " + arrayClass.getName());
          writeFloatArray((float[]) value);
          break;
        case DOUBLE:
          Preconditions.checkArgument(elementClass == double.class,
                  "Cannot write as double array: " + arrayClass.getName());
          writeDoubleArray((double[]) value);
          break;
        default:
          throw new IllegalArgumentException("Cannot write " +
                  descriptor.getJavaType() + " array: " + arrayClass.getName());
      }
    }

    protected void writeBooleanArray(boolean[] array) {
      if (array.length > 0) {
        startArray();
        for (boolean element : array) {
          recordConsumer.addBoolean(element);
        }
        endArray();
      }
    }

    protected void writeByteArray(byte[] array) {
      if (array.length > 0) {
        startArray();
        for (byte element : array) {
          recordConsumer.addInteger(element);
        }
        endArray();
      }
    }

    protected void writeShortArray(short[] array) {
      if (array.length > 0) {
        startArray();
        for (short element : array) {
          recordConsumer.addInteger(element);
        }
        endArray();
      }
    }

    protected void writeCharArray(char[] array) {
      if (array.length > 0) {
        startArray();
        for (char element : array) {
          recordConsumer.addInteger(element);
        }
        endArray();
      }
    }

    protected void writeIntArray(int[] array) {
      if (array.length > 0) {
        startArray();
        for (int element : array) {
          recordConsumer.addInteger(element);
        }
        endArray();
      }
    }

    protected void writeLongArray(long[] array) {
      if (array.length > 0) {
        startArray();
        for (long element : array) {
          recordConsumer.addLong(element);
        }
        endArray();
      }
    }

    protected void writeFloatArray(float[] array) {
      if (array.length > 0) {
        startArray();
        for (float element : array) {
          recordConsumer.addFloat(element);
        }
        endArray();
      }
    }

    protected void writeDoubleArray(double[] array) {
      if (array.length > 0) {
        startArray();
        for (double element : array) {
          recordConsumer.addDouble(element);
        }
        endArray();
      }
    }
  }

  /**
   * For backward-compatibility. This preserves how lists were written in 1.x.
   */
  private class TwoLevelListWriter extends ListWriter {
    @Override
    public void writeCollection(GroupType schema, Descriptors.FieldDescriptor descriptor,
                                Collection<?> array) {
      if (array.size() > 0) {
        recordConsumer.startField(OLD_LIST_REPEATED_NAME, 0);
        for (Object elt : array) {
          writeValue(schema.getType(0), descriptor, elt, Type.Repetition.OPTIONAL);
        }
        recordConsumer.endField(OLD_LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void writeObjectArray(GroupType type, Descriptors.FieldDescriptor descriptor,
                                    Object[] array) {
      if (array.length > 0) {
        recordConsumer.startField(OLD_LIST_REPEATED_NAME, 0);
        for (Object element : array) {
          writeValue(type.getType(0), descriptor, element, Type.Repetition.OPTIONAL);
        }
        recordConsumer.endField(OLD_LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void startArray() {
      recordConsumer.startField(OLD_LIST_REPEATED_NAME, 0);
    }

    @Override
    protected void endArray() {
      recordConsumer.endField(OLD_LIST_REPEATED_NAME, 0);
    }
  }

  private class ThreeLevelListWriter extends ListWriter {
    @Override
    protected void writeCollection(GroupType type, Descriptors.FieldDescriptor descriptor, Collection<?> collection) {
      if (collection.size() > 0) {
        recordConsumer.startField(LIST_REPEATED_NAME, 0);
        GroupType repeatedType = type.getType(0).asGroupType();
        Type elementType = repeatedType.getType(0);
        for (Object element : collection) {
          recordConsumer.startGroup(); // repeated group array, middle layer
          if (element != null) {
            recordConsumer.startField(LIST_ELEMENT_NAME, 0);
            writeValue(elementType, descriptor, element, Type.Repetition.OPTIONAL);
            recordConsumer.endField(LIST_ELEMENT_NAME, 0);
          } else if (!elementType.isRepetition(Type.Repetition.OPTIONAL)) {
            throw new RuntimeException(
                    "Null list element for " + descriptor.getName());
          }
          recordConsumer.endGroup();
        }
        recordConsumer.endField(LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void writeObjectArray(GroupType type, Descriptors.FieldDescriptor descriptor,
                                    Object[] array) {
      if (array.length > 0) {
        recordConsumer.startField(LIST_REPEATED_NAME, 0);
        GroupType repeatedType = type.getType(0).asGroupType();
        Type elementType = repeatedType.getType(0);
        for (Object element : array) {
          recordConsumer.startGroup(); // repeated group array, middle layer
          if (element != null) {
            recordConsumer.startField(LIST_ELEMENT_NAME, 0);
            writeValue(elementType, descriptor, element, Type.Repetition.OPTIONAL);
            recordConsumer.endField(LIST_ELEMENT_NAME, 0);
          } else if (!elementType.isRepetition(Type.Repetition.OPTIONAL)) {
            throw new RuntimeException(
                    "Null list element for " + descriptor.getName());
          }
          recordConsumer.endGroup();
        }
        recordConsumer.endField(LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void startArray() {
      recordConsumer.startField(LIST_REPEATED_NAME, 0);
      recordConsumer.startGroup(); // repeated group array, middle layer
      recordConsumer.startField(LIST_ELEMENT_NAME, 0);
    }

    @Override
    protected void endArray() {
      recordConsumer.endField(LIST_ELEMENT_NAME, 0);
      recordConsumer.endGroup();
      recordConsumer.endField(LIST_REPEATED_NAME, 0);
    }
  }

  @SuppressWarnings("unchecked")
  private void writeValue(Type type, Descriptors.FieldDescriptor descriptor, Object value) {
    writeValue(type, descriptor, value, getRepetition(descriptor));
  }
  @SuppressWarnings("unchecked")
  private void writeValue(Type type, Descriptors.FieldDescriptor descriptor, Object value, Type.Repetition repetition) {
    if (repetition == Type.Repetition.REPEATED) {
      listWriter.writeList(type.asGroupType(), descriptor, value);
    } else {
      JavaType javaType = descriptor.getJavaType();
      if (javaType.equals(JavaType.BOOLEAN)) {
        recordConsumer.addBoolean((Boolean) value);
      } else if (javaType.equals(JavaType.INT)) {
        if (value instanceof Character) {
          recordConsumer.addInteger((Character) value);
        } else {
          recordConsumer.addInteger(((Number) value).intValue());
        }
      } else if (javaType.equals(JavaType.LONG)) {
        recordConsumer.addLong(((Number) value).longValue());
      } else if (javaType.equals(JavaType.FLOAT)) {
        recordConsumer.addFloat(((Number) value).floatValue());
      } else if (javaType.equals(JavaType.DOUBLE)) {
        recordConsumer.addDouble(((Number) value).doubleValue());
//    } else if (javaType.equals(JavaType.BYTES)) {
//      if (value instanceof byte[]) {
//        recordConsumer.addBinary(Binary.fromReusedByteArray((byte[]) value));
//      } else {
//        recordConsumer.addBinary(Binary.fromReusedByteBuffer((ByteBuffer) value));
//      }
      } else if (javaType.equals(JavaType.STRING)) {
        recordConsumer.addBinary(Binary.fromString(value.toString()));
      } else if (javaType.equals(JavaType.MESSAGE)) {
        writeRecord(type.asGroupType(), descriptor.getMessageType(), (Message) value);
      }
    }
  }

  private void writeRecord(GroupType schema, Descriptors.Descriptor descriptor,
                           Message record) {
    recordConsumer.startGroup();
    writeRecordFields(schema, descriptor, record);
    recordConsumer.endGroup();
  }

  private void writeRecordFields(GroupType schema, Descriptors.Descriptor descriptor,
                                 Message record) {
    List<Type> fields = schema.getFields();
    List<Descriptors.FieldDescriptor> fieldDescriptors = descriptor.getFields();
    for (int fieldIndex = 0; fieldIndex < fieldDescriptors.size(); fieldIndex++) {
      Descriptors.FieldDescriptor fieldDescriptor = fieldDescriptors.get(fieldIndex);
      Type fieldType = fields.get(fieldIndex);
      Object value = record.getField(fieldDescriptor);
      if (value != null) {
        recordConsumer.startField(fieldType.getName(), fieldIndex);
        writeValue(fieldType, fieldDescriptor, value);
        recordConsumer.endField(fieldType.getName(), fieldIndex);
      } else if (fieldType.isRepetition(Type.Repetition.REQUIRED)) {
        throw new RuntimeException("Null-value for required field: " + fieldDescriptor.getName());
      }
    }
  }

  private Type.Repetition getRepetition(Descriptors.FieldDescriptor descriptor) {
    if (descriptor.isRequired()) {
      return Type.Repetition.REQUIRED;
    } else if (descriptor.isRepeated()) {
      return Type.Repetition.REPEATED;
    } else {
      return Type.Repetition.OPTIONAL;
    }
  }
}

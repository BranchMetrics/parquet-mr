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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.proto.ProtoWriteSupport.LIST_ELEMENT_NAME;
import static org.apache.parquet.proto.ProtoWriteSupport.OLD_LIST_REPEATED_NAME;
import static org.apache.parquet.schema.OriginalType.ENUM;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * <p/>
 * Converts a Protocol Buffer Descriptor into a Parquet schema.
 *
 * @author Hao Gao
 */
public class ProtoSchemaConverterV2 {
  private final int MAX_DEPTH = 8;

  private final boolean writeOldListStructure;

  public ProtoSchemaConverterV2() {
    writeOldListStructure = false;
  }

  public MessageType convert(Class<? extends Message> protobufClass) {
    Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(protobufClass);
    return new MessageType(descriptor.getName(), convertFields(descriptor.getFields(), MAX_DEPTH));
  }

  private List<Type> convertFields(List<Descriptors.FieldDescriptor> fieldDescriptors, int depth) {
    List<Type> types = new ArrayList<Type>();
    if (depth <= 0) {
      return types;
    }
    depth--;
    for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
      types.add(convertField(fieldDescriptor, depth));
    }
    return types;
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

  private Type convertField(Descriptors.FieldDescriptor descriptor, int depth) {
    return convertField(descriptor, depth, getRepetition(descriptor), descriptor.getName());
  }

  private Type convertField(Descriptors.FieldDescriptor descriptor, int depth, Type.Repetition repetition, String fieldName) {
    if (repetition == Type.Repetition.REPEATED) {
      if (writeOldListStructure) {
        return ConversionPatterns.listType(Type.Repetition.OPTIONAL, fieldName,
                convertField(descriptor, depth, Type.Repetition.OPTIONAL, OLD_LIST_REPEATED_NAME));
      } else {
        return ConversionPatterns.listOfElements(Type.Repetition.OPTIONAL, fieldName,
                convertField(descriptor, depth, Type.Repetition.OPTIONAL, LIST_ELEMENT_NAME));
      }
    } else {
      JavaType javaType = descriptor.getJavaType();
      switch (javaType) {
        case BOOLEAN : return primitive(fieldName, BOOLEAN, repetition);
        case INT : return primitive(fieldName, INT32, repetition);
        case LONG : return primitive(fieldName, INT64, repetition);
        case FLOAT : return primitive(fieldName, FLOAT, repetition);
        case DOUBLE: return primitive(fieldName, DOUBLE, repetition);
        case BYTE_STRING: return primitive(fieldName, BINARY, repetition);
        case STRING: return primitive(fieldName, BINARY, repetition, UTF8);
        case MESSAGE: {
          return new GroupType(repetition, fieldName, convertFields(descriptor.getMessageType().getFields(), depth));
        }
        case ENUM: return primitive(fieldName, BINARY, repetition, ENUM);
        default:
          throw new UnsupportedOperationException("Cannot convert Protocol Buffer: unknown type " + javaType);
      }
    }
  }

  private PrimitiveType primitive(String name,
                                  PrimitiveType.PrimitiveTypeName primitive, Type.Repetition repetition,
                                  OriginalType originalType) {
    return new PrimitiveType(repetition, primitive, name, originalType);
  }

  private PrimitiveType primitive(String name,
                                  PrimitiveType.PrimitiveTypeName primitive, Type.Repetition repetition) {
    return new PrimitiveType(repetition, primitive, name, null);
  }
}

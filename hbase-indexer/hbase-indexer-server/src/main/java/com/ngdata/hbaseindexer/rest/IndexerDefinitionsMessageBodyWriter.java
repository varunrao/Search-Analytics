/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer.rest;

import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.impl.IndexerDefinitionJsonSerDeser;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;

@Provider
public class IndexerDefinitionsMessageBodyWriter implements MessageBodyWriter<Collection<IndexerDefinition>> {
    @Override
    public long getSize(Collection<IndexerDefinition> indices, Class<?> type,
                        Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations,
                               MediaType mediaType) {
        if (Collection.class.isAssignableFrom(type) && mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
            if (genericType instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType)genericType;
                if (Arrays.asList(parameterizedType.getActualTypeArguments()).contains(IndexerDefinition.class)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void writeTo(Collection<IndexerDefinition> indices, Class<?> type,
                        Type genericType, Annotation[] annotations, MediaType mediaType,
                        MultivaluedMap<String, Object> httpHeaders, OutputStream outputStream)
            throws IOException, WebApplicationException {
        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        IndexerDefinitionJsonSerDeser converter = IndexerDefinitionJsonSerDeser.INSTANCE;

        for (IndexerDefinition index : indices) {
            array.add(converter.toJson(index));
        }

        ObjectMapper objectMapper = new ObjectMapper();
        IOUtils.write(objectMapper.writeValueAsBytes(array), outputStream);
    }
}

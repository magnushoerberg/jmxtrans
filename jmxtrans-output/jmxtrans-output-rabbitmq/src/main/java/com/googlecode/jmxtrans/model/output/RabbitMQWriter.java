/**
 * The MIT License
 * Copyright © 2010 JmxTrans team
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.googlecode.jmxtrans.model.output;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import static com.fasterxml.jackson.core.JsonEncoding.UTF8;
import com.google.common.collect.ImmutableList;

import com.googlecode.jmxtrans.model.OutputWriter;
import com.googlecode.jmxtrans.model.ValidationException;
import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.model.Server;
import static com.googlecode.jmxtrans.model.naming.KeyUtils.getKeyString;
import static com.googlecode.jmxtrans.model.naming.StringUtils.cleanupStr;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.Map.Entry;
import java.io.ByteArrayOutputStream;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This low latency and thread safe output writer sends data to a host/port combination
 * in the AMQP protocol.
 *
 * @see <a href="http://https://www.rabbitmq.com/tutorials/tutorial-three-java.html">Publish to RabbitMQ</a>
 */

public class RabbitMQWriter extends BaseOutputWriter {
    private static final Logger log = LoggerFactory.getLogger(RabbitMQWriter.class);

    private static final String DEFAULT_ROOT_PREFIX = "servers";

    private final String rootPrefix ;
    private final String uri ;
    private final ImmutableList<String> typeNames ;
    private final JsonFactory jsonFactory;

    @JsonCreator
    public RabbitMQWriter(
            @JsonProperty("uri") String uri,
            @JsonProperty("rootPrefix") String rootPrefix,
            @JsonProperty("typeNames") ImmutableList<String> typeNames,
            @JsonProperty("booleanAsNumber") boolean booleanAsNumber,
            @JsonProperty("settings") Map<String, Object> settings,
            @JsonProperty("debug") boolean debug) {
        super(typeNames, booleanAsNumber, debug, settings);
        this.typeNames = typeNames;
        this.uri = uri;
        this.jsonFactory = new JsonFactory();
        this.rootPrefix = firstNonNull(rootPrefix, DEFAULT_ROOT_PREFIX, "");
            }

    @Override
    public void validateSetup(Server server, Query query) throws ValidationException {}

    @Override
    protected void internalWrite(Server server, Query query, ImmutableList<Result> results) throws Exception {
        // Iterating through the list of query results
        Channel pubChan = createChannel();

        for (Result result : results) {
            Map<String, Object> resultValues = result.getValues();
            for (Map.Entry<String, Object> values : resultValues.entrySet()) {
            }
        }
        String msg = createJsonMessage(query, results);
        String rk = server.getAlias() + "." + query.getResultAlias();
        pubChan.basicPublish("amq.topic", rk, null, msg.getBytes());
    }

    private Channel createChannel() throws Exception {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setUri(this.uri);
        Connection connection = cf.newConnection();
        return connection.createChannel();
    }

    private String createJsonMessage(Query query, ImmutableList<Result> results) throws Exception {
        try (
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                JsonGenerator generator = jsonFactory.createGenerator(out, UTF8)
            )
        {
            generator.writeStartObject();
            String prevName = "";
            for (Result result : results) {
                Map<String, Object> resultValues = result.getValues();
                String name = query.makeTypeNameValueString(this.typeNames, result.getTypeName());
                if (!prevName.equals(name)) {
                    if (!prevName.equals("")) {
                        generator.writeEndObject();
                    }
                    generator.writeObjectFieldStart(name);
                }
                for (Map.Entry<String, Object> entry : resultValues.entrySet()) {
                    Object value = entry.getValue();
                    generator.writeStringField(entry.getKey(), value.toString());
                }
                log.info(prevName, name);
                prevName = name;
            }
            generator.writeEndObject();
            generator.close();
            return out.toString("UTF-8");
        }
    }
}

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
import com.google.common.collect.ImmutableList;

import com.googlecode.jmxtrans.model.OutputWriter;
import com.googlecode.jmxtrans.model.OutputWriterAdapter;
import com.googlecode.jmxtrans.model.OutputWriterFactory;
import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.model.Server;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.Map.Entry;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import lombok.ToString;

/**
 * This low latency and thread safe output writer sends data to a host/port combination
 * in the AMQP protocol.
 *
 * @see <a href="http://https://www.rabbitmq.com/tutorials/tutorial-three-java.html">Publish to RabbitMQ</a>
 */

@ToString
public class RabbitMQWriter implements OutputWriterFactory {
	private static final Logger log = LoggerFactory.getLogger(RabbitMQWriter.class);

    private final String uri ;
    private final ImmutableList<String> typeNames;
    private final boolean booleanAsNumber;
    private final boolean debug;

    @JsonCreator
    public RabbitMQWriter(
            @JsonProperty("uri") String uri,
            @JsonProperty("typeNames") ImmutableList<String> typeNames,
            @JsonProperty("booleanAsNumber") boolean booleanAsNumber,
            @JsonProperty("debug") boolean debug) {
        this.uri = uri;
        this.typeNames = typeNames;
        this.booleanAsNumber = booleanAsNumber;
        this.debug = debug;
        System.out.println(this.uri);
            }

    @Override
    public OutputWriter create() {
        return new W(this.uri);
    }

    public static class W extends OutputWriterAdapter {
        private final String uri;

        public W (String uri) {
            this.uri = uri;
        }

        private Channel createChannel() throws Exception {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setUri(this.uri);
            Connection connection = cf.newConnection();
            return connection.createChannel();
        }

        @Override
        public void doWrite(Server server, Query query, Iterable<Result> results) throws Exception {
            // Iterating through the list of query results
            Channel pubChan = createChannel();

            for (Result result : results) {
                System.out.println(result);
                //Map<String, Object> resultValues = result.getValues();
                //for (Map.Entry<String, Object> values : resultValues.entrySet()) {
                    //Object value = values.getValue();
                    //String rk = "";
                    //if (result.getAttributeName().equals(values.getKey())) {
                        //rk = result.getAttributeName();
                    //} else {
                        //rk = result.getAttributeName() + "_" + values.getKey();
                    //}
                    //pubChan.basicPublish("amq.topic", rk, null, value.toString().getBytes());
                //}
            }
        }

    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.mule.transport;

import org.mule.api.ConnectionException;
import org.mule.api.MuleContext;
import org.mule.api.callback.SourceCallback;
import org.mule.api.transformer.Transformer;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.types.DataTypeFactory;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.List;

public class ZeroMQTransport {

    public enum ExchangePattern {
        REQUEST_RESPONSE, ONE_WAY, PUBLISH, SUBSCRIBE, PUSH, PULL
    }

    public enum SocketOperation {
        BIND, CONNECT
    }

    private MuleContext muleContext;

    private ZMQ.Context zmqContext;
    private ZMQ.Socket zmqSocket;
    private ExchangePattern exchangePattern;
    private Transformer objectToByteArrayTransformer;

    public void setMuleContext(MuleContext muleContext) {
        this.muleContext = muleContext;
    }

    public void connect(ExchangePattern exchangePattern, SocketOperation socketOperation, String address, String filter, boolean isInbound)
            throws ConnectionException {

        if (!isInbound) {
            this.exchangePattern = exchangePattern;

            switch (exchangePattern) {
                case REQUEST_RESPONSE:
                    zmqSocket = requestResponseOnOutbound(socketOperation, address);
                    break;
                case PUBLISH:
                    zmqSocket = publish(socketOperation, address);
                    break;
                case ONE_WAY:
                    zmqSocket = push(socketOperation, address);
                    break;
                case SUBSCRIBE:
                    zmqSocket = subscribe(socketOperation, address, filter);
                    break;
                case PUSH:
                    zmqSocket = push(socketOperation, address);
                    break;
                case PULL:
                    zmqSocket = pull(socketOperation, address);
                    break;
            }
        }
    }

    public void initialise() throws TransformerException {
        zmqContext = ZMQ.context(1);
        objectToByteArrayTransformer = muleContext.getRegistry().lookupTransformer(DataTypeFactory.create((Object.class)), DataTypeFactory.create((byte[].class)));
    }

    public void destroy() {
        zmqContext.term();
    }

    public void disconnect() {
        zmqSocket.close();
    }

    public boolean isConnected() {
        if (zmqSocket != null)
            return true;
        else
            return false;
    }

    public String connectionId() {
        return "001";
    }

    public Object outboundEndpoint(Boolean multipart, Object payload) throws Exception {
        Object message;

        switch (exchangePattern) {
            case REQUEST_RESPONSE:
                send(zmqSocket, payload, multipart);
                message = receive(zmqSocket);
                break;
            case PUBLISH:
                send(zmqSocket, payload, multipart);
                message = payload;
                break;
            case ONE_WAY:
                send(zmqSocket, payload, multipart);
                message = payload;
                break;
            case SUBSCRIBE:
                message = receive(zmqSocket);
                break;
            case PUSH:
                send(zmqSocket, payload, multipart);
                message = payload;
                break;
            case PULL:
                message = receive(zmqSocket);
                break;
            default:
                throw new UnsupportedOperationException();
        }

        return message;
    }

    public void inboundEndpoint(ExchangePattern exchangePattern, SocketOperation socketOperation, String address, String filter, Boolean multipart, SourceCallback callback) throws Exception {
        ZMQ.Poller poller = zmqContext.poller(1);

        switch (exchangePattern) {

            case REQUEST_RESPONSE:
                zmqSocket = requestResponseOnInbound(socketOperation, address);
                poller.register(zmqSocket, ZMQ.Poller.POLLIN);
                while (true) {
                    poller.poll();
                    Object response = callback.process(receive(zmqSocket));
                    send(zmqSocket, response, multipart);
                }

            case ONE_WAY:
                zmqSocket = pull(socketOperation, address);
                poller.register(zmqSocket, ZMQ.Poller.POLLIN);
                while (true) {
                    poller.poll();
                    callback.process(receive(zmqSocket));
                }

            case SUBSCRIBE:
                zmqSocket = subscribe(socketOperation, address, filter);
                poller.register(zmqSocket, ZMQ.Poller.POLLIN);
                while (true) {
                    poller.poll();
                    callback.process(receive(zmqSocket));
                }

            case PUBLISH:
                throw new UnsupportedOperationException();

            case PUSH:
                throw new UnsupportedOperationException();

            case PULL:
                zmqSocket = pull(socketOperation, address);
                poller.register(zmqSocket, ZMQ.Poller.POLLIN);
                while (true) {
                    poller.poll();
                    callback.process(receive(zmqSocket));
                }

            default:
                throw new UnsupportedOperationException();
        }
    }

    private void receive(ZMQ.Socket zmqSocket, List<byte[]> messageParts) {
        messageParts.add(zmqSocket.recv(0));

        while (zmqSocket.hasReceiveMore()) {
            receive(zmqSocket, messageParts);
        }
    }

    private void send(ZMQ.Socket zmqSocket, Object payload, boolean multipart) throws Exception {
        if (!multipart) {
            zmqSocket.send((byte[]) objectToByteArrayTransformer.transform(payload), 0);
        } else {
            List messageParts = (List) payload;

            for (int i = 0; i < (messageParts.size() - 1); i++) {
                zmqSocket.send((byte[]) objectToByteArrayTransformer.transform(messageParts.get(i)), ZMQ.SNDMORE);
            }

            zmqSocket.send((byte[]) objectToByteArrayTransformer.transform(messageParts.get(messageParts.size() - 1)), 0);
        }
    }

    private Object receive(ZMQ.Socket zmqSocket) {
        List<byte[]> messageParts = new ArrayList<byte[]>();

        messageParts.add(zmqSocket.recv(0));

        while (zmqSocket.hasReceiveMore()) {
            receive(zmqSocket, messageParts);
        }

        if (messageParts.size() < 2) {
            return messageParts.get(0);
        } else {
            return messageParts;
        }
    }

    private ZMQ.Socket subscribe(SocketOperation socketOperation, String address, String filter) {
        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.SUB);

        prepare(zmqSocket, socketOperation, address);

        if (filter != null) {
            zmqSocket.subscribe(filter.getBytes());
        } else {
            zmqSocket.subscribe(new byte[]{});
        }

        return zmqSocket;
    }

    private ZMQ.Socket pull(SocketOperation socketOperation, String address) {
        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PULL);
        prepare(zmqSocket, socketOperation, address);

        return zmqSocket;
    }

    private ZMQ.Socket push(SocketOperation socketOperation, String address) {
        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUSH);
        prepare(zmqSocket, socketOperation, address);

        return zmqSocket;
    }

    private ZMQ.Socket requestResponseOnInbound(SocketOperation socketOperation, String address) {
        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.REP);
        prepare(zmqSocket, socketOperation, address);

        return zmqSocket;
    }

    private ZMQ.Socket requestResponseOnOutbound(SocketOperation socketOperation, String address) {
        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.REQ);
        prepare(zmqSocket, socketOperation, address);

        return zmqSocket;
    }

    private ZMQ.Socket publish(SocketOperation socketOperation, String address) {
        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        String[] subscribers = address.split(";");

        for (String subscriber : subscribers) {
            prepare(zmqSocket, socketOperation, subscriber);
        }

        return zmqSocket;
    }

    private void prepare(ZMQ.Socket zmqSocket, SocketOperation socketOperation, String address) {
        if (socketOperation.equals(SocketOperation.BIND)) {
            zmqSocket.bind(address);
        } else {
            zmqSocket.connect(address);
        }
    }

}

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
import org.mule.api.MuleException;
import org.mule.api.callback.SourceCallback;
import org.mule.api.config.ThreadingProfile;
import org.mule.api.context.WorkManager;
import org.mule.api.transformer.Transformer;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.types.DataTypeFactory;
import org.mule.util.concurrent.ThreadNameHelper;
import org.zeromq.ZMQ;
import org.zeromq.ZMQQueue;

import javax.resource.spi.work.Work;
import java.util.ArrayList;
import java.util.List;

public class ZeroMQTransport {

    public enum ExchangePattern {
        REQUEST_RESPONSE, ONE_WAY, PUBLISH, SUBSCRIBE, PUSH, PULL
    }

    public enum SocketOperation {
        BIND, CONNECT
    }

    private final static String INBOUND_RECEIVER_WORKER_ADDRESS = "inproc://receivers";
    private final static String DISPATCHER_WORKER_ADDRESS = "inproc://dispatcher";
    private final static String REQUESTOR_WORKER_ADDRESS = "inproc://requestor";
    private final static String OUTBOUND_RECEIVER_WORKER_ADDRESS = "inproc://receivers";
    private final static int WAIT_SOCKET_BIND = 50;

    private MuleContext muleContext;
    private ZMQ.Context zmqContext;
    private ExchangePattern exchangePattern;
    private Transformer objectToByteArrayTransformer;
    private ThreadingProfile receiverThreadingProfile;
    private Boolean connected = false;

    public void setReceiverThreadingProfile(ThreadingProfile receiverThreadingProfile) {
        this.receiverThreadingProfile = receiverThreadingProfile;
    }

    public void setMuleContext(MuleContext muleContext) {
        this.muleContext = muleContext;
    }

    public void connect(ExchangePattern exchangePattern, SocketOperation socketOperation, String address, String filter, boolean isInbound, boolean multipart)
            throws ConnectionException {

        if (!isInbound) {

            this.exchangePattern = exchangePattern;

            switch (exchangePattern) {
                case REQUEST_RESPONSE:
                    RequestorWorker requestor = new RequestorWorker(multipart, address, socketOperation);

                    new Thread(requestor).start();

                    try {
                        while (!requestor.isStarted()) {
                            Thread.sleep(WAIT_SOCKET_BIND);
                        }
                    } catch (InterruptedException e) {
                        throw new ConnectionException(null, null, null, e);
                    }

                    break;

                case PUBLISH:
                    new Thread(new DispatcherWorker(multipart, address, socketOperation, ZMQ.PUB)).start();
                    break;

                case ONE_WAY:
                    new Thread(new DispatcherWorker(multipart, address, socketOperation, ZMQ.PUSH)).start();
                    break;

                case SUBSCRIBE:
                    startOutboundReceiverWorker(new OutboundReceiverWorker(multipart, address, socketOperation, ZMQ.SUB, filter));
                    break;

                case PUSH:
                    new Thread(new DispatcherWorker(multipart, address, socketOperation, ZMQ.PUSH)).start();
                    break;

                case PULL:
                    startOutboundReceiverWorker(new OutboundReceiverWorker(multipart, address, socketOperation, ZMQ.PULL, null));
                    break;
            }

            connected = true;
        }
    }

    public void initialise() throws TransformerException {
        zmqContext = ZMQ.context(1);
        objectToByteArrayTransformer = muleContext.getRegistry().lookupTransformer(DataTypeFactory.create((Object.class)), DataTypeFactory.create((byte[].class)));
    }

    public void destroy() {
        zmqContext.term();
    }

    public boolean isConnected() {
        if (connected)
            return true;
        else
            return false;
    }

    public String connectionId() {
        return "001";
    }

    public Object outboundEndpoint(Boolean multipart, Object payload) throws Exception {
        Object message;
        ZMQ.Socket dispatcher;

        switch (exchangePattern) {
            case REQUEST_RESPONSE:
                ZMQ.Socket requestor = createSocket(ZMQ.REQ, SocketOperation.CONNECT, REQUESTOR_WORKER_ADDRESS);
                send(requestor, payload, multipart);
                message = receive(requestor);
                requestor.close();
                break;

            case PUBLISH:
                dispatcher = createSocket(ZMQ.PUSH, SocketOperation.CONNECT, DISPATCHER_WORKER_ADDRESS);
                send(dispatcher, payload, true);
                message = payload;
                dispatcher.close();
                break;

            case ONE_WAY:
                dispatcher = createSocket(ZMQ.PUSH, SocketOperation.CONNECT, DISPATCHER_WORKER_ADDRESS);
                send(dispatcher, payload, true);
                message = payload;
                dispatcher.close();
                break;

            case SUBSCRIBE:
                ZMQ.Socket subscriber = createSocket(ZMQ.PULL, SocketOperation.CONNECT, OUTBOUND_RECEIVER_WORKER_ADDRESS);
                message = receive(subscriber);
                subscriber.close();
                break;

            case PUSH:
                dispatcher = createSocket(ZMQ.PUSH, SocketOperation.CONNECT, DISPATCHER_WORKER_ADDRESS);
                send(dispatcher, payload, true);
                message = payload;
                dispatcher.close();
                break;

            case PULL:
                ZMQ.Socket puller = createSocket(ZMQ.PULL, SocketOperation.CONNECT, OUTBOUND_RECEIVER_WORKER_ADDRESS);
                message = receive(puller);
                puller.close();
                break;

            default:
                throw new UnsupportedOperationException();
        }

        return message;
    }

    public void inboundEndpoint(ExchangePattern exchangePattern, SocketOperation socketOperation, String address, String filter, Boolean multipart, SourceCallback callback) throws Exception {
        ZMQ.Socket zmqSocket;

        switch (exchangePattern) {

            case REQUEST_RESPONSE:

                WorkManager receiverWorkManager = initReceiverWorkManager();
                ZMQ.Socket clients = createSocket(ZMQ.ROUTER, socketOperation, address);
                ZMQ.Socket workers = createSocket(ZMQ.DEALER, SocketOperation.BIND, INBOUND_RECEIVER_WORKER_ADDRESS);
                ZMQQueue zmqQueue = new ZMQQueue(zmqContext, clients, workers);

                for (int i = 0; i < receiverThreadingProfile.getMaxThreadsActive(); i++) {
                    receiverWorkManager.scheduleWork(new InboundWorker(multipart, callback));
                }

                zmqQueue.run();

                break;

            case ONE_WAY:
                zmqSocket = createSocket(ZMQ.PULL, socketOperation, address);
                poll(zmqSocket, multipart, callback);

            case SUBSCRIBE:
                zmqSocket = createSocket(ZMQ.SUB, socketOperation, address, filter);
                poll(zmqSocket, multipart, callback);

            case PUBLISH:
                throw new UnsupportedOperationException();

            case PUSH:
                throw new UnsupportedOperationException();

            case PULL:
                zmqSocket = createSocket(ZMQ.PULL, socketOperation, address);
                poll(zmqSocket, multipart, callback);

            default:
                throw new UnsupportedOperationException();
        }

    }

    private void startOutboundReceiverWorker(OutboundReceiverWorker outboundReceiverWorker) throws ConnectionException {
        new Thread(outboundReceiverWorker).start();

        try {
            while (!outboundReceiverWorker.isStarted()) {
                Thread.sleep(WAIT_SOCKET_BIND);
            }
        } catch (InterruptedException e) {
            throw new ConnectionException(null, null, null, e);
        }
    }

    private void poll(ZMQ.Socket zmqSocket, Boolean multipart, SourceCallback callback) throws Exception {
        ZMQ.Poller poller = createPoller(zmqSocket);
        WorkManager workManager = initReceiverWorkManager();

        while (true) {
            poller.poll();
            workManager.scheduleWork(new InboundWorker(multipart, callback, receive(zmqSocket)));
        }
    }

    private WorkManager initReceiverWorkManager() throws MuleException {
        if (receiverThreadingProfile == null) {
            receiverThreadingProfile = muleContext.getDefaultMessageReceiverThreadingProfile();
        }

        WorkManager workManager = receiverThreadingProfile.createWorkManager(ThreadNameHelper.receiver(muleContext, "ZeroMQConnector"), muleContext.getConfiguration().getShutdownTimeout());
        workManager.start();

        return workManager;
    }

    private void receive(ZMQ.Socket zmqSocket, List<byte[]> messageParts) {
        messageParts.add(zmqSocket.recv(0));

        if (zmqSocket.hasReceiveMore()) {
            receive(zmqSocket, messageParts);
        }
    }

    private void send(ZMQ.Socket zmqSocket, Object payload, boolean multipart) throws Exception {
        if (!multipart || (multipart && !(payload instanceof List))) {
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

        if (zmqSocket.hasReceiveMore()) {
            receive(zmqSocket, messageParts);
        }

        if (messageParts.size() < 2) {
            return messageParts.get(0);
        } else {
            return messageParts;
        }
    }

    private ZMQ.Socket createSocket(int socketType, SocketOperation socketOperation, String address) {
        return createSocket(socketType, socketOperation, address, null);
    }

    private ZMQ.Socket createSocket(int socketType, SocketOperation socketOperation, String address, String filter) {
        ZMQ.Socket zmqSocket = zmqContext.socket(socketType);

        if (socketType == ZMQ.PUB) {
            String[] subscribers = address.split(";");

            for (String subscriber : subscribers) {
                prepare(zmqSocket, socketOperation, subscriber);
            }
        } else {
            prepare(zmqSocket, socketOperation, address);
        }

        if (socketType == ZMQ.SUB) {
            if (filter != null) {
                zmqSocket.subscribe(filter.getBytes());
            } else {
                zmqSocket.subscribe(new byte[]{});
            }
        }

        return zmqSocket;
    }

    private ZMQ.Poller createPoller(ZMQ.Socket zmqSocket) {
        ZMQ.Poller poller = zmqContext.poller(1);
        poller.register(zmqSocket);

        return poller;
    }

    private void prepare(ZMQ.Socket zmqSocket, SocketOperation socketOperation, String address) {
        if (socketOperation.equals(SocketOperation.BIND)) {
            zmqSocket.bind(address);
        } else {
            zmqSocket.connect(address);
        }
    }

    private class InboundWorker implements Work {

        private SourceCallback callback;
        private Boolean multipart;
        private Object message;

        public InboundWorker(Boolean multipart, SourceCallback callback) {
            this.callback = callback;
            this.multipart = multipart;
        }

        public InboundWorker(Boolean multipart, SourceCallback callback, Object message) {
            this.callback = callback;
            this.multipart = multipart;
            this.message = message;
        }

        @Override
        public void release() {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void run() {
            ZMQ.Socket workerSocket = zmqContext.socket(ZMQ.REP);

            try {
                if (message != null) {
                    callback.process(message);
                } else {
                    workerSocket.connect(INBOUND_RECEIVER_WORKER_ADDRESS);
                    Object response = callback.process(ZeroMQTransport.this.receive(workerSocket));

                    send(workerSocket, response, multipart);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                workerSocket.close();
            }
        }
    }

    private class RequestorWorker implements Runnable {

        private boolean started;
        private String address;
        private Boolean multipart;
        private SocketOperation socketOperation;

        public RequestorWorker(Boolean multipart, String address, SocketOperation socketOperation) {
            this.multipart = multipart;
            this.socketOperation = socketOperation;
            this.address = address;
        }

        public boolean isStarted() {
            return started;
        }

        @Override
        public void run() {
            try {
                ZMQ.Socket outboundEndpoint = createSocket(ZMQ.REQ, socketOperation, address);
                ZMQ.Socket requestor = createSocket(ZMQ.REP, SocketOperation.BIND, REQUESTOR_WORKER_ADDRESS);
                ZMQ.Poller poller = createPoller(requestor);
                started = true;

                while (true) {
                    poller.poll();
                    send(outboundEndpoint, receive(requestor), multipart);
                    send(requestor, receive(outboundEndpoint), multipart);
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class OutboundReceiverWorker implements Runnable {

        private boolean started;
        private String address;
        private Boolean multipart;
        private SocketOperation socketOperation;
        private String filter;
        private int socketType;

        public OutboundReceiverWorker(Boolean multipart, String address, SocketOperation socketOperation, int socketType, String filter) {
            this.multipart = multipart;
            this.socketOperation = socketOperation;
            this.address = address;
            this.filter = filter;
            this.socketType = socketType;
        }

        public boolean isStarted() {
            return started;
        }

        @Override
        public void run() {
            try {
                ZMQ.Socket outboundEndpointSocket = createSocket(socketType, socketOperation, address, filter);
                ZMQ.Socket puller = createSocket(ZMQ.PUSH, SocketOperation.BIND, OUTBOUND_RECEIVER_WORKER_ADDRESS);
                ZMQ.Poller poller = createPoller(outboundEndpointSocket);
                started = true;

                while (true) {
                    poller.poll();
                    send(puller, receive(outboundEndpointSocket), multipart);
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class DispatcherWorker implements Runnable {

        private String address;
        private Boolean multipart;
        private SocketOperation socketOperation;
        private int socketType;

        public DispatcherWorker(Boolean multipart, String address, SocketOperation socketOperation, int socketType) {
            this.multipart = multipart;
            this.socketOperation = socketOperation;
            this.address = address;
            this.socketType = socketType;
        }

        @Override
        public void run() {
            ZMQ.Socket source = ZeroMQTransport.this.createSocket(ZMQ.PULL, SocketOperation.BIND, DISPATCHER_WORKER_ADDRESS);
            ZMQ.Socket sink = ZeroMQTransport.this.createSocket(socketType, socketOperation, address);
            ZMQ.Poller poller = createPoller(source);
            try {
                while (true) {
                    poller.poll();
                    send(sink, receive(source), multipart);
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }
}

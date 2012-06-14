package org.mule.transport;

import org.mule.api.ConnectionException;
import org.mule.api.annotations.*;
import org.mule.api.annotations.lifecycle.Start;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Optional;
import org.mule.api.annotations.param.Payload;
import org.mule.api.callback.SourceCallback;
import org.mule.transformer.simple.ObjectToByteArray;
import org.zeromq.ZMQ;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

@Connector(name = "zeromq", schemaVersion = "1.0-SNAPSHOT")
public class ZeroMQTransport {

    public enum ExchangePattern {
        REQUEST_RESPONSE, ONE_WAY, PUBLISH, SUBSCRIBE, PUSH, PULL
    }

    public enum SocketOperation {
        BIND, CONNECT
    }

    private ZMQ.Context zmqContext;
    private ZMQ.Socket zmqSocket;
    private ExchangePattern exchangePattern;
    private ObjectToByteArray objectToByteArray;

    @Connect
    public void connect(@ConnectionKey ExchangePattern exchangePattern, @ConnectionKey SocketOperation socketOperation, @ConnectionKey String address, @ConnectionKey String filter, boolean isInbound)
            throws ConnectionException {

        if (!isInbound) {
            this.exchangePattern = exchangePattern;

            switch (exchangePattern) {
                case REQUEST_RESPONSE:
                    zmqSocket = requestResponseOutbound(socketOperation, address);
                    break;
                case PUBLISH:
                    zmqSocket = publish(socketOperation, address);
                    break;
                case ONE_WAY:
                    zmqSocket = requestResponseOutbound(socketOperation, address);
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

    @Start
    public void initialise() {
        zmqContext = ZMQ.context(1);
        objectToByteArray = new ObjectToByteArray();
    }

    @PreDestroy
    public void destroy() {
        zmqContext.term();
    }

    @Disconnect
    public void disconnect() {
        zmqSocket.close();
    }

    @ValidateConnection
    public boolean isConnected() {
        if (zmqSocket != null)
            return true;
        else
            return false;
    }

    @ConnectionIdentifier
    public String connectionId() {
        return "001";
    }

    @Processor
    public byte[] outboundEndpoint(@Payload byte[] payload) throws Exception {
        byte[] message;

        switch (exchangePattern) {
            case REQUEST_RESPONSE:
                zmqSocket.send(payload, 0);
                message = zmqSocket.recv(0);
                break;
            case PUBLISH:
                zmqSocket.send(payload, 0);
                message = payload;
                break;
            case ONE_WAY:
                zmqSocket.send(payload, 0);
                message = payload;
                break;
            case SUBSCRIBE:
                message = zmqSocket.recv(0);
                break;
            case PUSH:
                zmqSocket.send(payload, 0);
                message = payload;
                break;
            case PULL:
                message = zmqSocket.recv(0);
                break;
            default:
                throw new UnsupportedOperationException();
        }

        return message;
    }

    @Source
    public void inboundEndpoint(ExchangePattern exchangePattern, SocketOperation socketOperation, String address, @Optional String filter, SourceCallback callback) throws Exception {
        Object message;

        switch (exchangePattern) {

            case REQUEST_RESPONSE:
                zmqSocket = requestResponseInbound(socketOperation, address);
                message = receive(zmqSocket);
                Object response = callback.process(message);
                zmqSocket.send((byte[]) objectToByteArray.transform(response), 0);
                break;

            case ONE_WAY:
                zmqSocket = requestResponseInbound(socketOperation, address);
                message = receive(zmqSocket);
                callback.process(message);
                break;

            case SUBSCRIBE:
                zmqSocket = subscribe(socketOperation, address, filter);
                message = receive(zmqSocket);
                callback.process(message);
                break;

            case PUBLISH:
                throw new UnsupportedOperationException();

            case PUSH:
                throw new UnsupportedOperationException();

            case PULL:
                zmqSocket = pull(socketOperation, address);
                message = receive(zmqSocket);
                callback.process(message);
                break;

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

    private Object receive(ZMQ.Socket zmqSocket) {
        List<byte[]> messageParts = new ArrayList<byte[]>();

        messageParts.add(zmqSocket.recv(0));

        while (zmqSocket.hasReceiveMore()) {
            receive(zmqSocket, messageParts);
        }

        if (messageParts.size() > 1) {
            return messageParts;
        }
        else {
            return messageParts.get(0);
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

    private ZMQ.Socket requestResponseInbound(SocketOperation socketOperation, String address) {
        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.REP);
        prepare(zmqSocket, socketOperation, address);

        return zmqSocket;
    }

    private ZMQ.Socket requestResponseOutbound(SocketOperation socketOperation, String address) {
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

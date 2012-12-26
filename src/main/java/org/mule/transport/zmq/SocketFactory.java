/*
 * Copyright 2012 Claude Mamo
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.mule.transport.zmq;

import org.zeromq.ZMQ;

public class SocketFactory {

    private String address;
    private String filter;
    private String identity;
    private int socketType;
    private ZeroMQTransport.SocketOperation socketOperation;
    private ZMQ.Context zmqContext;

    public SocketFactory(ZMQ.Context zmqContext, String address, int socketType, ZeroMQTransport.SocketOperation socketOperation) {
        this.zmqContext = zmqContext;
        this.address = address;
        this.socketType = socketType;
        this.socketOperation = socketOperation;
    }

    public String getFilter() {
        return filter;
    }

    public SocketFactory setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getIdentity() {
        return identity;
    }

    public SocketFactory setIdentity(String identity) {
        this.identity = identity;
        return this;
    }

    public ZMQ.Socket createSocket() {
        ZMQ.Socket zmqSocket = zmqContext.socket(socketType);

        if (identity != null) {
            zmqSocket.setIdentity(identity.getBytes());
        }

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

    private void prepare(ZMQ.Socket zmqSocket, ZeroMQTransport.SocketOperation socketOperation, String address) {
        if (socketOperation.equals(ZeroMQTransport.SocketOperation.BIND)) {
            zmqSocket.bind(address);
        } else {
            zmqSocket.connect(address);
        }
    }
}

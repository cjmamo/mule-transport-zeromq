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

import javax.resource.spi.work.Work;

public abstract class ZeroMQWorker implements Work {

    protected boolean started;
    protected String address;
    protected Boolean multipart;
    protected ZeroMQTransport.SocketOperation socketOperation;
    protected String filter;
    protected int socketType;
    protected String identity;

    public ZeroMQWorker(Boolean multipart, String address, ZeroMQTransport.SocketOperation socketOperation, int socketType, String filter) {
        this.multipart = multipart;
        this.socketOperation = socketOperation;
        this.address = address;
        this.filter = filter;
        this.socketType = socketType;
    };

    public ZeroMQWorker(Boolean multipart, String address, ZeroMQTransport.SocketOperation socketOperation, int socketType, String filter, String identity) {
        this.multipart = multipart;
        this.socketOperation = socketOperation;
        this.address = address;
        this.filter = filter;
        this.socketType = socketType;
        this.identity = identity;
    };


    @Override
    public void release() {

    }

    @Override
    public void run() {

    }

    public boolean isStarted() {
        return started;
    }

}

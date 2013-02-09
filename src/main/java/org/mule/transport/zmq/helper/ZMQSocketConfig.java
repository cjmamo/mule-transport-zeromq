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
package org.mule.transport.zmq.helper;

public class ZMQSocketConfig {

    private String address;
    private ZMQURIConstants.SocketOperation socketOperation;
    private ZMQURIConstants.SocketType socketType;
    private String filter;
    private String identity;

    public ZMQURIConstants.SocketType getSocketType() {
        return socketType;
    }

    public ZMQSocketConfig setSocketType(ZMQURIConstants.SocketType socketType) {
        this.socketType = socketType;
        return this;
    }

    public String getAddress() {
        return address;
    }

    public ZMQSocketConfig setAddress(String address) {
        this.address = address;
        return this;
    }

    public ZMQSocketConfig(String address) {
        this.address = address;
    }

    public ZMQSocketConfig() {
    }

    public ZMQURIConstants.SocketOperation getSocketOperation() {
        return socketOperation;
    }

    public ZMQSocketConfig setSocketOperation(ZMQURIConstants.SocketOperation socketOperation) {
        this.socketOperation = socketOperation;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public ZMQSocketConfig setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getIdentity() {
        return identity;
    }

    public ZMQSocketConfig setIdentity(String identity) {
        this.identity = identity;
        return this;
    }
}

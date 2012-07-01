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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mule.DefaultMuleMessage;
import org.mule.api.MuleEvent;
import org.mule.api.MuleEventContext;
import org.mule.api.MuleMessage;
import org.mule.api.transport.PropertyScope;
import org.mule.construct.Flow;
import org.mule.module.client.MuleClient;
import org.mule.tck.functional.EventCallback;
import org.mule.tck.functional.FunctionalTestComponent;
import org.mule.tck.junit4.FunctionalTestCase;
import org.mule.tck.junit4.rule.DynamicPort;
import org.mule.tck.junit4.rule.FreePortFinder;
import org.mule.util.ArrayUtils;
import org.mule.util.concurrent.Latch;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ZeroMQTransportTest extends FunctionalTestCase implements EventCallback {

    private static final int CONNECT_WAIT = 1000;

    @Rule
    public DynamicPort requestResponseOnInboundBindFlowPort = new DynamicPort("requestresponse.oninbound.bind.flow.port");

    @Rule
    public DynamicPort requestResponseOnInboundConnectFlowPort = new DynamicPort("requestresponse.oninbound.connect.flow.port");

    @Rule
    public DynamicPort subscribeOnInboundNoFilterFlowPort = new DynamicPort("subscribe.oninbound.nofilter.flow.port");

    @Rule
    public DynamicPort subscribeOnInboundFilterFlowPort = new DynamicPort("subscribe.oninbound.filter.flow.port");

    @Rule
    public DynamicPort pullOnInboundBindFlowPort = new DynamicPort("pull.oninbound.bind.flow.port");

    @Rule
    public DynamicPort pullOnInboundConnectFlowPort = new DynamicPort("pull.oninbound.connect.flow.port");

    @Rule
    public DynamicPort requestResponseOnOutboundBindFlowPort = new DynamicPort("requestresponse.onoutbound.bind.flow.port");

    @Rule
    public DynamicPort subscribeOnOutboundNoFilterFlowPort = new DynamicPort("subscribe.onoutbound.nofilter.flow.port");

    @Rule
    public DynamicPort subscribeOnOutboundFilterFlowPort = new DynamicPort("subscribe.onoutbound.filter.flow.port");

    @Rule
    public DynamicPort requestResponseOnOutboundConnectFlowPort = new DynamicPort("requestresponse.onoutbound.connect.flow.port");

    @Rule
    public DynamicPort publishFlowSubscriber1Port = new DynamicPort("publish.flow.subscriber1.port");

    @Rule
    public DynamicPort publishFlowSubscriber2Port = new DynamicPort("publish.flow.subscriber2.port");

    @Rule
    public DynamicPort pullOnOutboundBindFlowPort = new DynamicPort("pull.onoutbound.bind.flow.port");

    @Rule
    public DynamicPort pushBindFlowPort = new DynamicPort("push.bind.flow.port");

    @Rule
    public DynamicPort pullOnOutboundConnectFlowPort = new DynamicPort("pull.onoutbound.connect.flow.port");

    @Rule
    public DynamicPort pushConnectFlowPort = new DynamicPort("push.connect.flow.port");

    @Rule
    public DynamicPort multipleSourcesSubscriberFlowPort = new DynamicPort("multiplesources.subscriber.flow.port");

    @Rule
    public DynamicPort multipleSourcesPullFlowPort = new DynamicPort("multiplesources.pull.flow.port");

    @Rule
    public DynamicPort multiPartMessageOnInboundFlowPort = new DynamicPort("multipartmessage.oninbound.flow.port");

    @Rule
    public DynamicPort multiPartMessageOnOutboundFlowPort = new DynamicPort("multipartmessage.onoutbound.flow.port");

    @Rule
    public DynamicPort identityFlowPort = new DynamicPort("identity.flow.port");

    private static ZMQ.Context zmqContext;

    @Override
    protected String getConfigResources() {
        return "mule-config.xml";
    }

    public ZeroMQTransportTest() {
        this.setDisposeContextPerClass(true);
    }

    @BeforeClass
    public static void oneTimeSetUp() {
        zmqContext = ZMQ.context(1);
    }

    @AfterClass
    public static void oneTimeTearDown() {
        zmqContext.term();
    }

    @Test
    public void testMultiPartMessageOnOutbound() throws Exception {
        MuleClient client = new MuleClient(muleContext);
        List messageParts = new ArrayList<String>();

        messageParts.add("The quick brown fox ");
        messageParts.add("jumps over the lazy dog");

        client.dispatch("vm://multipartmessage.onoutbound", new DefaultMuleMessage(messageParts, muleContext));

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PULL);
        zmqSocket.setReceiveTimeOut(RECEIVE_TIMEOUT);
        zmqSocket.connect("tcp://localhost:" + multiPartMessageOnOutboundFlowPort.getNumber());

        byte[] firstPart = zmqSocket.recv(0);
        assertNotNull(firstPart);
        assertEquals("The quick brown fox ", new String(firstPart));
        assertTrue(zmqSocket.hasReceiveMore());

        byte[] secondPart = zmqSocket.recv(0);
        assertNotNull(firstPart);
        assertEquals("jumps over the lazy dog", new String(secondPart));
        assertFalse(zmqSocket.hasReceiveMore());

        zmqSocket.close();
    }

    @Test
    public void testMultiPartMessageOnInbound() throws Exception {
        MuleClient client = new MuleClient(muleContext);

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUSH);
        zmqSocket.connect("tcp://localhost:" + multiPartMessageOnInboundFlowPort.getNumber());
        zmqSocket.send("The quick brown fox ".getBytes(), ZMQ.SNDMORE);
        zmqSocket.send("jumps over the lazy dog".getBytes(), 0);
        zmqSocket.close();

        MuleMessage subscribeMessage = client.request("vm://multipartmessage.oninbound", RECEIVE_TIMEOUT);
        assertTrue(subscribeMessage.getPayload() instanceof List);
        List<byte[]> messageParts = (List<byte[]>) subscribeMessage.getPayload();
        assertEquals("The quick brown fox ", new String(messageParts.get(0)));
        assertEquals("jumps over the lazy dog", new String(messageParts.get(1)));
    }

    @Test
    public void testMultipleSources() throws Exception {
        MuleClient client = new MuleClient(muleContext);

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.bind("tcp://*:" + multipleSourcesSubscriberFlowPort.getNumber());
        Thread.sleep(CONNECT_WAIT);
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        zmqSocket.close();

        MuleMessage subscribeMessage = client.request("vm://multiplesources", RECEIVE_TIMEOUT);
        assertEquals("The quick brown fox", subscribeMessage.getPayloadAsString());

        zmqSocket = zmqContext.socket(ZMQ.PUSH);
        zmqSocket.connect("tcp://localhost:" + multipleSourcesPullFlowPort.getNumber());
        zmqSocket.send("jumps over the lazy dog".getBytes(), 0);
        zmqSocket.close();

        MuleMessage pullMessage = client.request("vm://multiplesources", RECEIVE_TIMEOUT);
        assertEquals("jumps over the lazy dog", pullMessage.getPayloadAsString());

    }

    @Test
    public void testRequestResponseOnOutboundBind() throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.REP);
                zmqSocket.connect("tcp://localhost:" + requestResponseOnOutboundBindFlowPort.getNumber());
                byte[] request = zmqSocket.recv(0);
                zmqSocket.send(ArrayUtils.addAll(request, " jumps over the lazy dog".getBytes()), 0);
                zmqSocket.close();
            }
        }).start();

        runFlowWithPayloadAndExpect("RequestResponseOnOutboundBindFlow", "The quick brown fox jumps over the lazy dog", "The quick brown fox");
    }

    @Test
    public void testPullOnOutboundBind() throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUSH);
                zmqSocket.connect("tcp://localhost:" + pullOnOutboundBindFlowPort.getNumber());
                zmqSocket.send("The quick brown fox jumps over the lazy dog".getBytes(), 0);
                zmqSocket.close();

            }
        }).start();

        runFlowWithPayloadAndExpect("PullOnOutboundBindFlow", "The quick brown fox jumps over the lazy dog", null);
    }

    @Test
    public void testPushConnect() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        class Puller implements Runnable {

            byte[] message;

            public byte[] getMessage() {
                return message;
            }

            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PULL);
                zmqSocket.bind("tcp://*:" + pushConnectFlowPort.getNumber());
                message = zmqSocket.recv(0);
                zmqSocket.close();
                messageLatch.countDown();
            }
        }

        Puller puller = new Puller();
        new Thread(puller).start();

        runFlowWithPayload("PushConnectFlow", "The quick brown fox jumps over the lazy dog");
        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
        assertEquals("The quick brown fox jumps over the lazy dog", new String(puller.getMessage()));
    }

    @Test
    public void testPullOnOutboundConnect() throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUSH);
                zmqSocket.bind("tcp://*:" + pullOnOutboundConnectFlowPort.getNumber());
                zmqSocket.send("The quick brown fox jumps over the lazy dog".getBytes(), 0);
                zmqSocket.close();
            }
        }).start();

        runFlowWithPayloadAndExpect("PullOnOutboundConnectFlow", "The quick brown fox jumps over the lazy dog", null);
    }

    @Test
    public void testPushBind() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        class Puller implements Runnable {

            byte[] message;

            public byte[] getMessage() {
                return message;
            }

            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PULL);
                zmqSocket.connect("tcp://localhost:" + pushBindFlowPort.getNumber());
                message = zmqSocket.recv(0);
                zmqSocket.close();
                messageLatch.countDown();
            }
        }

        Puller puller = new Puller();
        new Thread(puller).start();

        runFlowWithPayload("PushBindFlow", "The quick brown fox jumps over the lazy dog");
        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
        assertEquals("The quick brown fox jumps over the lazy dog", new String(puller.getMessage()));
    }

    @Test
    public void testIdentity() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("IdentityFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                assertEquals("The quick brown fox", ((FunctionalTestComponent) component).getLastReceivedMessage());
                messageLatch.countDown();
            }
        });

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.connect("tcp://localhost:" + identityFlowPort.getNumber());
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        zmqSocket.close();
        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }


    @Test
    public void testDynamicEndpoint() throws Exception {
        final Integer freePort = new FreePortFinder(6000, 7000).find();

        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.REP);
                zmqSocket.connect("tcp://localhost:" + freePort);
                byte[] request = zmqSocket.recv(0);
                zmqSocket.send(ArrayUtils.addAll(request, " jumps over the lazy dog".getBytes()), 0);
                zmqSocket.close();
            }
        }).start();

        Map<String, Object> properties = new HashMap<String, Object>();

        properties.put("address", "tcp://*:" + freePort);
        properties.put("socket-operation", "bind");
        properties.put("exchange-pattern", "request-response");

        runFlowWithPayloadAndPropertiesAndExpect("DynamicEndpointFlow", "The quick brown fox jumps over the lazy dog", "The quick brown fox", properties);
    }

    @Test
    public void testRequestResponseOnOutboundConnect() throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.REP);
                zmqSocket.bind("tcp://*:" + requestResponseOnOutboundConnectFlowPort.getNumber());
                byte[] request = zmqSocket.recv(0);
                zmqSocket.send(ArrayUtils.addAll(request, " jumps over the lazy dog".getBytes()), 0);
                zmqSocket.close();
            }
        }).start();

        runFlowWithPayloadAndExpect("RequestResponseOnOutboundConnectFlow", "The quick brown fox jumps over the lazy dog", "The quick brown fox");
    }

    @Test
    public void testPublish() throws Exception {

        class Subscriber implements Runnable {

            byte[] message;

            private CountDownLatch messageLatch;
            private int portNo;

            public Subscriber(int portNo) {
                this.portNo = portNo;
                messageLatch = new Latch();
            }

            public byte[] getMessage() {
                return message;
            }

            public CountDownLatch getMessageLatch() {
                return messageLatch;
            }

            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.SUB);
                zmqSocket.connect("tcp://localhost:" + portNo);
                zmqSocket.subscribe(new byte[]{});
                message = zmqSocket.recv(0);
                zmqSocket.close();
                messageLatch.countDown();
            }
        }

        Subscriber subscriber1 = new Subscriber(publishFlowSubscriber1Port.getNumber());
        Subscriber subscriber2 = new Subscriber(publishFlowSubscriber2Port.getNumber());

        new Thread(subscriber1).start();
        new Thread(subscriber2).start();

        runFlowWithPayload("PublishFlow", "The quick brown fox");
        assertTrue(subscriber1.getMessageLatch().await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
        assertTrue(subscriber2.getMessageLatch().await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));

        assertEquals("The quick brown fox jumps over the lazy dog", new String(subscriber1.getMessage()));
        assertEquals("The quick brown fox jumps over the lazy dog", new String(subscriber2.getMessage()));
    }

    @Test
    public void testRequestResponseOnInboundBind() throws Exception {

        getFunctionalTestComponent("RequestResponseOnInboundBindFlow").setEventCallback(this);

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.REQ);
        zmqSocket.setReceiveTimeOut(RECEIVE_TIMEOUT);
        zmqSocket.connect("tcp://localhost:" + requestResponseOnInboundBindFlowPort.getNumber());
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        byte[] response = zmqSocket.recv(0);

        zmqSocket.close();

        assertNotNull(response);
        assertEquals("The quick brown fox jumps over the lazy dog", new String(response));
    }

    @Test
    public void testRequestResponseOnInboundConnect() throws Exception {
        getFunctionalTestComponent("RequestResponseOnInboundConnectFlow").setEventCallback(this);

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.REQ);
        zmqSocket.setReceiveTimeOut(RECEIVE_TIMEOUT);
        zmqSocket.bind("tcp://*:" + requestResponseOnInboundConnectFlowPort.getNumber());
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        byte[] response = zmqSocket.recv(0);

        zmqSocket.close();

        assertNotNull(response);
        assertEquals("The quick brown fox jumps over the lazy dog", new String(response));
    }

    @Test
    public void testSubscribeOnInboundNoFilter() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("SubscribeOnInboundNoFilterFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                assertEquals("The quick brown fox", ((FunctionalTestComponent) component).getLastReceivedMessage());
                messageLatch.countDown();
            }
        });

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.bind("tcp://*:" + subscribeOnInboundNoFilterFlowPort.getNumber());
        Thread.sleep(CONNECT_WAIT);
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        zmqSocket.close();
        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testSubscribeOnOutboundNoFilter() throws Exception {

        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
                zmqSocket.bind("tcp://*:" + subscribeOnOutboundNoFilterFlowPort.getNumber());
                try {
                    Thread.sleep(CONNECT_WAIT);
                    zmqSocket.send("The quick brown fox jumps over the lazy dog".getBytes(), 0);
                } catch (Exception e) {
                    throw new RuntimeException();
                } finally {
                    zmqSocket.close();
                }
            }
        }).start();

        runFlowWithPayloadAndExpect("SubscribeOnOutboundNoFilterFlow", "The quick brown fox jumps over the lazy dog", null);
    }

    @Test
    public void testSubscribeOnOutboundFilterReject() throws Exception {

        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("SubscribeOnOutboundFilterFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                messageLatch.countDown();
            }
        });

        MuleClient client = new MuleClient(muleContext);
        client.dispatch("vm://subscribe.onoutbound.filter", new DefaultMuleMessage(null, muleContext));
        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
                zmqSocket.bind("tcp://*:" + subscribeOnOutboundFilterFlowPort.getNumber());
                try {
                    Thread.sleep(CONNECT_WAIT);
                    zmqSocket.send("Bar".getBytes(), 0);
                } catch (Exception e) {
                    throw new RuntimeException();
                } finally {
                    zmqSocket.close();
                }
            }
        }).start();
        assertFalse(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testSubscribeOnOutboundFilterAccept() throws Exception {

        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("SubscribeOnOutboundFilterFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                messageLatch.countDown();
            }
        });

        MuleClient client = new MuleClient(muleContext);
        client.dispatch("vm://subscribe.onoutbound.filter", new DefaultMuleMessage(null, muleContext));

        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
                zmqSocket.bind("tcp://*:" + subscribeOnOutboundFilterFlowPort.getNumber());
                try {
                    Thread.sleep(CONNECT_WAIT);
                    zmqSocket.send("Foo".getBytes(), 0);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    zmqSocket.close();
                }
            }
        }).start();

        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
        getFunctionalTestComponent("SubscribeOnOutboundFilterFlow").getLastReceivedMessage().equals("Foo");
    }

    @Test
    public void testSubscribeOnInboundFilterReject() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("SubscribeOnInboundFilterFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                messageLatch.countDown();
            }
        });

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.bind("tcp://*:" + subscribeOnInboundFilterFlowPort.getNumber());
        Thread.sleep(CONNECT_WAIT);
        zmqSocket.send("Bar".getBytes(), 0);
        zmqSocket.close();
        assertFalse(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testSubscribeOnInboundFilterAccept() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("SubscribeOnInboundFilterFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                assertEquals("Foo", ((FunctionalTestComponent) component).getLastReceivedMessage());
                messageLatch.countDown();
            }
        });

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.bind("tcp://*:" + subscribeOnInboundFilterFlowPort.getNumber());
        Thread.sleep(CONNECT_WAIT);
        zmqSocket.send("Foo".getBytes(), 0);
        zmqSocket.close();
        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testPullOnInboundBind() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("PullOnInboundBindFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                assertEquals("The quick brown fox", ((FunctionalTestComponent) component).getLastReceivedMessage());
                messageLatch.countDown();
            }
        });

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.connect("tcp://localhost:" + pullOnInboundBindFlowPort.getNumber());
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        zmqSocket.close();
        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testPullOnInboundConnect() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("PullOnInboundConnectFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                assertEquals("The quick brown fox", ((FunctionalTestComponent) component).getLastReceivedMessage());
                messageLatch.countDown();
            }
        });

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.bind("tcp://*:" + pullOnInboundConnectFlowPort.getNumber());
        Thread.sleep(CONNECT_WAIT);
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        zmqSocket.close();
        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    protected <T, U> void runFlowWithPayloadAndExpect(String flowName, T expect, U payload) throws Exception {
        Flow flow = lookupFlowConstruct(flowName);
        MuleEvent event = getTestEvent(payload);
        MuleEvent responseEvent = flow.process(event);

        assertEquals(expect, responseEvent.getMessage().getPayloadAsString());
    }

    protected <T, U> void runFlowWithPayloadAndPropertiesAndExpect(String flowName, T expect, U payload, Map<String, Object> properties) throws Exception {
        Flow flow = lookupFlowConstruct(flowName);
        MuleEvent event = getTestEvent(payload);
        event.getMessage().addProperties(properties, PropertyScope.INBOUND);
        MuleEvent responseEvent = flow.process(event);

        assertEquals(expect, responseEvent.getMessage().getPayloadAsString());
    }

    protected <T, U> void runFlowWithPayload(String flowName, U payload) throws Exception {
        Flow flow = lookupFlowConstruct(flowName);
        MuleEvent event = getTestEvent(payload);
        flow.process(event);
    }

    protected Flow lookupFlowConstruct(String name) {
        return (Flow) muleContext.getRegistry().lookupFlowConstruct(name);
    }

    @Override
    public void eventReceived(MuleEventContext context, Object component) throws Exception {
        assertEquals("The quick brown fox", ((FunctionalTestComponent) component).getLastReceivedMessage());
    }
}

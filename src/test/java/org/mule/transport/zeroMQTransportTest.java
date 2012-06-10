package org.mule.transport;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mule.DefaultMuleMessage;
import org.mule.api.MuleEvent;
import org.mule.api.MuleEventContext;
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ZeroMQTransportTest extends FunctionalTestCase implements EventCallback {

    private static final int CONNECT_WAIT = 1000;

    @Rule
    public DynamicPort requestResponseInboundBindFlowPort = new DynamicPort("request.response.inbound.bind.flow.port");

    @Rule
    public DynamicPort requestResponseInboundConnectFlowPort = new DynamicPort("request.response.inbound.connect.flow.port");

    @Rule
    public DynamicPort subscribeInboundNoFilterFlowPort = new DynamicPort("subscribe.inbound.nofilter.flow.port");

    @Rule
    public DynamicPort subscribeInboundFilterFlowPort = new DynamicPort("subscribe.inbound.filter.flow.port");

    @Rule
    public DynamicPort pullInboundBindFlowPort = new DynamicPort("pull.inbound.bind.flow.port");

    @Rule
    public DynamicPort pullInboundConnectFlowPort = new DynamicPort("pull.inbound.connect.flow.port");

    @Rule
    public DynamicPort requestResponseOutboundBindFlowPort = new DynamicPort("request.response.outbound.bind.flow.port");

    @Rule
    public DynamicPort subscribeOutboundNoFilterFlowPort = new DynamicPort("subscribe.outbound.nofilter.flow.port");

    @Rule
    public DynamicPort subscribeOutboundFilterFlowPort = new DynamicPort("subscribe.outbound.filter.flow.port");

    @Rule
    public DynamicPort requestResponseOutboundConnectFlowPort = new DynamicPort("request.response.outbound.connect.flow.port");

    @Rule
    public DynamicPort publishFlowSubscriber1Port = new DynamicPort("publish.flow.subscriber1.port");

    @Rule
    public DynamicPort publishFlowSubscriber2Port = new DynamicPort("publish.flow.subscriber2.port");

    @Rule
    public DynamicPort dynamicEndpointFlowPort = new DynamicPort("dynamic.endpoint.flow.port");

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
    public void testRequestResponseOutboundBind() throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.REP);
                zmqSocket.connect("tcp://localhost:" + requestResponseOutboundBindFlowPort.getNumber());
                byte[] request = zmqSocket.recv(0);
                zmqSocket.send(ArrayUtils.addAll(request, " jumps over the lazy dog".getBytes()), 0);
                zmqSocket.close();
            }
        }).start();

        runFlowWithPayloadAndExpect("RequestResponseOutboundBindFlow", "The quick brown fox jumps over the lazy dog", "The quick brown fox");
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
    public void testRequestResponseOutboundConnect() throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.REP);
                zmqSocket.bind("tcp://*:" + requestResponseOutboundConnectFlowPort.getNumber());
                byte[] request = zmqSocket.recv(0);
                zmqSocket.send(ArrayUtils.addAll(request, " jumps over the lazy dog".getBytes()), 0);
                zmqSocket.close();
            }
        }).start();

        runFlowWithPayloadAndExpect("RequestResponseOutboundConnectFlow", "The quick brown fox jumps over the lazy dog", "The quick brown fox");
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
    public void testRequestResponseInboundBind() throws Exception {

        getFunctionalTestComponent("RequestResponseInboundBindFlow").setEventCallback(this);

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.REQ);
        zmqSocket.setReceiveTimeOut(RECEIVE_TIMEOUT);
        zmqSocket.connect("tcp://localhost:" + requestResponseInboundBindFlowPort.getNumber());
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        byte[] response = zmqSocket.recv(0);

        zmqSocket.close();

        assertNotNull(response);
        assertEquals("The quick brown fox jumps over the lazy dog", new String(response));
    }

    @Test
    public void testRequestResponseInboundConnect() throws Exception {
        getFunctionalTestComponent("RequestResponseInboundConnectFlow").setEventCallback(this);

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.REQ);
        zmqSocket.setReceiveTimeOut(RECEIVE_TIMEOUT);
        zmqSocket.bind("tcp://*:" + requestResponseInboundConnectFlowPort.getNumber());
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        byte[] response = zmqSocket.recv(0);

        zmqSocket.close();

        assertNotNull(response);
        assertEquals("The quick brown fox jumps over the lazy dog", new String(response));
    }

    @Test
    public void testSubscribeInboundNoFilter() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("SubscribeInboundNoFilterFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                assertEquals("The quick brown fox", ((FunctionalTestComponent) component).getLastReceivedMessage());
                messageLatch.countDown();
            }
        });

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.bind("tcp://*:" + subscribeInboundNoFilterFlowPort.getNumber());
        Thread.sleep(CONNECT_WAIT);
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        zmqSocket.close();
        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testSubscribeOutboundNoFilter() throws Exception {

        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
                zmqSocket.bind("tcp://*:" + subscribeOutboundNoFilterFlowPort.getNumber());
                try {
                    Thread.sleep(CONNECT_WAIT);
                } catch (Exception e) {
                    throw new RuntimeException();
                }
                zmqSocket.send("The quick brown fox jumps over the lazy dog".getBytes(), 0);
                zmqSocket.close();
            }
        }).start();

        runFlowWithPayloadAndExpect("SubscribeOutboundNoFilterFlow", "The quick brown fox jumps over the lazy dog", null);
    }

    @Test
    public void testSubscribeOutboundFilterReject() throws Exception {

        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("SubscribeOutboundFilterFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                messageLatch.countDown();
            }
        });

        MuleClient client = new MuleClient(muleContext);
        client.dispatch("vm://subscribe.outbound.filter", new DefaultMuleMessage(null, muleContext));
        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
                zmqSocket.bind("tcp://*:" + subscribeOutboundFilterFlowPort.getNumber());
                try {
                    Thread.sleep(CONNECT_WAIT);
                    zmqSocket.send("Bar".getBytes(), 0);
                } catch (Exception e) {
                    throw new RuntimeException();
                }
                finally {
                    zmqSocket.close();
                }
            }
        }).start();
        assertFalse(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testSubscribeOutboundFilterAccept() throws Exception {

        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("SubscribeOutboundFilterFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                messageLatch.countDown();
            }
        });

        MuleClient client = new MuleClient(muleContext);
        client.dispatch("vm://subscribe.outbound.filter", new DefaultMuleMessage(null, muleContext));

        new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
                zmqSocket.bind("tcp://*:" + subscribeOutboundFilterFlowPort.getNumber());
                try {
                    Thread.sleep(CONNECT_WAIT);
                    zmqSocket.send("Foo".getBytes(), 0);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                finally {
                    zmqSocket.close();
                }
            }
        }).start();

        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
        getFunctionalTestComponent("SubscribeOutboundFilterFlow").getLastReceivedMessage().equals("Foo");
    }

    @Test
    public void testSubscribeInboundFilterReject() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("SubscribeInboundFilterFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                messageLatch.countDown();
            }
        });

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.bind("tcp://*:" + subscribeInboundFilterFlowPort.getNumber());
        Thread.sleep(CONNECT_WAIT);
        zmqSocket.send("Bar".getBytes(), 0);
        zmqSocket.close();
        assertFalse(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testSubscribeInboundFilterAccept() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("SubscribeInboundFilterFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                assertEquals("Foo", ((FunctionalTestComponent) component).getLastReceivedMessage());
                messageLatch.countDown();
            }
        });

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.bind("tcp://*:" + subscribeInboundFilterFlowPort.getNumber());
        Thread.sleep(CONNECT_WAIT);
        zmqSocket.send("Foo".getBytes(), 0);
        zmqSocket.close();
        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testPullInboundBind() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("PullInboundBindFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                assertEquals("The quick brown fox", ((FunctionalTestComponent) component).getLastReceivedMessage());
                messageLatch.countDown();
            }
        });

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.connect("tcp://localhost:" + pullInboundBindFlowPort.getNumber());
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        zmqSocket.close();
        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testPullInboundConnect() throws Exception {
        final CountDownLatch messageLatch = new Latch();

        getFunctionalTestComponent("PullInboundConnectFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                assertEquals("The quick brown fox", ((FunctionalTestComponent) component).getLastReceivedMessage());
                messageLatch.countDown();
            }
        });

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.bind("tcp://*:" + pullInboundConnectFlowPort.getNumber());
        Thread.sleep(CONNECT_WAIT);
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        zmqSocket.close();
        assertTrue(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    /**
     * Run the flow specified by name using the specified payload and assert
     * equality on the expected output
     *
     * @param flowName The name of the flow to run
     * @param expect   The expected output
     * @param payload  The payload of the input event
     */
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

    /**
     * Retrieve a flow by name from the registry
     *
     * @param name Name of the flow to retrieve
     */
    protected Flow lookupFlowConstruct(String name) {
        return (Flow) muleContext.getRegistry().lookupFlowConstruct(name);
    }

    @Override
    public void eventReceived(MuleEventContext context, Object component) throws Exception {
        assertEquals("The quick brown fox", ((FunctionalTestComponent) component).getLastReceivedMessage());
    }
}

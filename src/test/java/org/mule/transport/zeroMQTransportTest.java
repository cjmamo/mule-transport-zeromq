package org.mule.transport;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mule.api.MuleEvent;
import org.mule.api.MuleEventContext;
import org.mule.construct.Flow;
import org.mule.tck.functional.EventCallback;
import org.mule.tck.functional.FunctionalTestComponent;
import org.mule.tck.junit4.FunctionalTestCase;
import org.mule.tck.junit4.rule.DynamicPort;
import org.mule.util.ArrayUtils;
import org.mule.util.concurrent.Latch;
import org.zeromq.ZMQ;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


public class ZeroMQTransportTest extends FunctionalTestCase implements EventCallback {

    private CountDownLatch messageLatch = new Latch();
    private CountDownLatch messageLatch2 = new Latch();
    private static final int CONNECT_WAIT = 2000;

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
    public DynamicPort requestResponseOutboundConnectFlowPort = new DynamicPort("request.response.outbound.connect.flow.port");

    @Rule
    public DynamicPort publishFlowPort = new DynamicPort("publish.flow.port");

    private static ZMQ.Context zmqContext;

    @Override
    protected String getConfigResources() {
        return "mule-config.xml";
    }

    public ZeroMQTransportTest() {
        this.setDisposeContextPerClass(true);
    }

    private String deserialize(byte[] data) throws Exception {
        InputStream in = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(in);
        return ois.readObject().toString();
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
//        Thread.sleep(5000);
        class Subscriber implements Runnable {

            byte[] message;

            public byte[] getMessage() {
                return message;
            }

            @Override
            public void run() {
                ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.SUB);
                zmqSocket.connect("tcp://localhost:" + publishFlowPort.getNumber());
                zmqSocket.subscribe(new byte[]{});
                message = zmqSocket.recv(0);
                zmqSocket.close();
                messageLatch2.countDown();
            }
        }

        new Thread(new Subscriber()).start();

        runFlowWithPayload("PublishFlow", "The quick brown fox jumps over the lazy dog");
        assertTrue(messageLatch2.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
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
        assertEquals("The quick brown fox jumps over the lazy dog", deserialize(response));
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
        assertEquals("The quick brown fox jumps over the lazy dog", deserialize(response));
    }

    @Test
    public void testSubscribeInboundNoFilter() throws Exception {
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
    public void testSubscribeInboundFilterReject() throws Exception {
        getFunctionalTestComponent("SubscribeInboundFilterFlow").setEventCallback(new EventCallback() {
            @Override
            public void eventReceived(MuleEventContext context, Object component) throws Exception {
                messageLatch.countDown();
            }
        });

        ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PUB);
        zmqSocket.bind("tcp://*:" + subscribeInboundFilterFlowPort.getNumber());
        Thread.sleep(CONNECT_WAIT);
        zmqSocket.send("The quick brown fox".getBytes(), 0);
        zmqSocket.close();
        assertFalse(messageLatch.await(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testSubscribeInboundFilterAccept() throws Exception {
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

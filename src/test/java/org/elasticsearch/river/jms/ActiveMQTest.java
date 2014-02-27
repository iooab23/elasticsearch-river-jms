package org.elasticsearch.river.jms;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ActiveMQTest {

    private static final String BROKER_URL = "vm://localhost";

    final String messageForTest1 = "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n"
            + "{ \"type1\" : { \"field1\" : \"value1\" } }\n"
            + "{ \"delete\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n"
            + "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n"
            + "{ \"type1\" : { \"field1\" : \"value1\" } }";

    final String messageForTest2 = "{ \"type2\" : { \"field2\" : \"value2\" } }";

    private static BrokerService broker;
    private static Client client;

    @BeforeClass
    public static void startUp() throws Exception {
        startActiveMQBroker();
        startElasticSearchInstance();
    }

    @AfterClass
    public static void shutDown() throws Exception {
        stopElasticSearchInstance();
        stopActiveMQBroker();
    }

    private static void startActiveMQBroker() throws Exception {
        broker = new BrokerService();
        broker.setUseShutdownHook(false);
        broker.setSystemExitOnShutdown(false);
        broker.setWaitForSlave(false);
        broker.start();
    }

    private static void stopActiveMQBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        System.out.println("Broker shut down");
    }

    private static void startElasticSearchInstance() throws IOException {

        Settings settings = ImmutableSettings.settingsBuilder().put("node.http.enabled", false).put("gateway.type", "none")
                .put("index.store.type", "memory").put("index.number_of_shards", 1).put("index.number_of_replicas", 1).build();

        Node node = NodeBuilder.nodeBuilder().local(true).settings(settings).client(false).node();
        client = node.client();
    }

    private static void stopElasticSearchInstance() {
        System.out.println("shutting down elasticsearch");
        client.close();
    }

    private static void clearTestingIndexes() {
        // Delete Testing indexes
        try {
            client.admin().indices().delete(new DeleteIndexRequest("test")).actionGet();
            client.admin().indices().flush(new FlushRequest("test")).actionGet();
            client.admin().indices().refresh(new RefreshRequest("test")).actionGet();
        } catch (Throwable e) {
            // e.printStackTrace();
        }
    }

    private void setRiverConfigurationSettings(Map<String, Map<String, Object>> settings) throws IOException {

        XContentBuilder settingsJson = jsonBuilder().startObject();
        settingsJson.field("type", "jms");
        for (String key : settings.keySet()) {
            settingsJson.field(key, settings.get(key));
        }
        System.out.println("SETTINGS: " + settingsJson.prettyPrint().string());
        try {
            client.admin().indices().delete(new DeleteIndexRequest("_river")).actionGet();
        } catch (Throwable e) {
        }
        client.prepareIndex("_river", "testRiver", "_meta").setCreate(false).setRefresh(true).setOperationThreaded(false)
                .setSource(settingsJson).execute().actionGet();
        client.admin().indices().flush(new FlushRequest("_river")).actionGet();
        client.admin().indices().refresh(new RefreshRequest("_river")).actionGet();
    }

    @Test
    public void testSimpleScenario() throws Exception {

        Map<String, Map<String, Object>> settings = new HashMap<String, Map<String, Object>>();

        Map<String, Object> jmsSetting = new HashMap<String, Object>();
        jmsSetting.put("jndiProviderUrl", BROKER_URL);
        jmsSetting.put("jndiContextFactory", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        jmsSetting.put("connectionFactory", "ElasticSearchConnFactory");
        jmsSetting.put("sourceType", "topic");
        jmsSetting.put("sourceName", JmsRiver.defaultSourceName);

        settings.put("jms", jmsSetting);

        clearTestingIndexes();
        setRiverConfigurationSettings(settings);
        Thread.sleep(5000l);

        // assure that the index is not yet there
        try {
            client.admin().indices().flush(new FlushRequest("test")).actionGet();
            client.admin().indices().refresh(new RefreshRequest("test")).actionGet();
            ListenableActionFuture<SearchResponse> future = client.prepareSearch("test").execute();
            Object o = future.actionGet();
            SearchResponse resp = (SearchResponse) o;
            Assert.assertEquals(0, resp.hits().getTotalHits());
        } catch (IndexMissingException idxExcp) {

        }

        // TODO connect to the ActiveMQ Broker and publish a message into the
        // default queue
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);

        try {
            Connection conn = factory.createConnection();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic queue = session.createTopic(JmsRiver.defaultSourceName);
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage(messageForTest1));

            producer.close();
            session.close();
            conn.close();
        } catch (JMSException e) {
            Assert.fail("JMS Exception");
        }

        Thread.sleep(5000l);

        {
            client.admin().indices().flush(new FlushRequest("test")).actionGet();
            client.admin().indices().refresh(new RefreshRequest("test")).actionGet();
            ListenableActionFuture<SearchResponse> future = client.prepareSearch("test").execute();
            Object o = future.actionGet();
            SearchResponse resp = (SearchResponse) o;
            Assert.assertEquals(1, resp.hits().getTotalHits());
            Assert.assertEquals("{ \"type1\" : { \"field1\" : \"value1\" } }", resp.hits().iterator().next()
                    .getSourceAsString());
        }

        System.out.println("Test complete");

    }

    @Test
    public void testNonBulkSimpleScenario() throws Exception {

        Map<String, Map<String, Object>> settings = new HashMap<String, Map<String, Object>>();
        Map<String, Object> jmsSettings = new HashMap<String, Object>();

        jmsSettings.put("jndiProviderUrl", BROKER_URL);
        jmsSettings.put("jndiContextFactory", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        jmsSettings.put("connectionFactory", "ElasticSearchConnFactory");
        jmsSettings.put("sourceType", "topic");
        jmsSettings.put("sourceName", JmsRiver.defaultSourceName);

        settings.put("jms", jmsSettings);

        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("_action", "index");
        indexSettings.put("_index", "test");
        indexSettings.put("_type", "type1");

        settings.put("index", indexSettings);

        clearTestingIndexes();
        setRiverConfigurationSettings(settings);

        Thread.sleep(5000l);

        // assure that the index is not yet there
        try {
            client.admin().indices().flush(new FlushRequest("test")).actionGet();
            client.admin().indices().refresh(new RefreshRequest("test")).actionGet();
            ListenableActionFuture<SearchResponse> future = client.prepareSearch("test").execute();
            Object o = future.actionGet();
            SearchResponse resp = (SearchResponse) o;
            Assert.assertEquals(0, resp.hits().getTotalHits());
        } catch (IndexMissingException idxExcp) {

        }

        // TODO connect to the ActiveMQ Broker and publish a message into the
        // default queue
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);

        try {
            Connection conn = factory.createConnection();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic queue = session.createTopic(JmsRiver.defaultSourceName);
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage(messageForTest2));

            producer.close();
            session.close();
            conn.close();
        } catch (JMSException e) {
            Assert.fail("JMS Exception");
        }

        Thread.sleep(5000l);

        {
            client.admin().indices().flush(new FlushRequest("test")).actionGet();
            client.admin().indices().refresh(new RefreshRequest("test")).actionGet();
            ListenableActionFuture<SearchResponse> future = client.prepareSearch("test").execute();
            Object o = future.actionGet();
            SearchResponse resp = (SearchResponse) o;
            Assert.assertEquals(1, resp.hits().getTotalHits());
            Assert.assertEquals(this.messageForTest2, resp.hits().iterator().next().getSourceAsString());
        }

        System.out.println("Test complete");

    }
}

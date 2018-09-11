package Worker;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;

import java.util.Enumeration;

import javax.jms.*;

public class SQSClass {
    
	private String queueName;
	private GetQueueUrlResult queueURL;
	private AWSCredentialsProvider credentialsProvider;
	private String REGION;

	private SQSConnectionFactory connectionFactory;
	private SQSConnection connection;
	private Session session;
	private MessageConsumer consumer;
	private MessageProducer producer;
	private QueueBrowser queueBrowser;
	
	public SQSClass(AWSCredentialsProvider credentialsProvider,
					String region, String queueName){
		this.credentialsProvider = credentialsProvider;
		this.REGION = region;
		this.queueName = queueName;

		this.connectionFactory = new SQSConnectionFactory(
				new ProviderConfiguration(),
				AmazonSQSClientBuilder.standard()
				.withRegion(REGION)
				.withCredentials(credentialsProvider)
		);

	}

	protected void createSqsQueue(){
		System.out.println("Creating a new SQS queue named: " + queueName);
		try {
			SQSConnection connection = connectionFactory.createConnection(credentialsProvider.getCredentials());
			AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
			if (!client.queueExists(queueName)) {
				client.createQueue(queueName);
			}
			this.queueURL = client.getQueueUrl(queueName);
		} catch (JMSException e) {
			System.out.println("failed to create a SQSconnection: " + e);
		}
	}

	protected void startSQSConnection(){
		try {
			this.connection = connectionFactory.createConnection();
			this.session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			Destination q = session.createQueue(queueName);
			this.consumer = session.createConsumer(q);
			this.producer = session.createProducer(q);
			
			System.out.println("starting a SQSconnection");
			connection.start();
		} catch (JMSException e) {
			System.out.println("failed to start a SQSconnection: " + e);
		}
	}

	public void closeSQSConnection(){
		try {
			System.out.println("closing a SQSconnection");
			connection.close();
		} catch (JMSException e) {
			System.out.println("failed to close a SQSConnection: " + e);
		}
	}
	
	public Message receiveMessage(){
		Message receivedMessage = null;
		try {
			receivedMessage = consumer.receive();
		} catch (JMSException e) {
			System.out.println("failed to receive a message: " +e);
		}
		return receivedMessage;
	}
	
	public void sendMessage(String msg){
		try {
			Message message = session.createTextMessage(msg);
			producer.send(message);
		} catch (JMSException e) {
			System.out.println("failed to send a message: " + e);
		}
	}
	
	public String getQueueName(){
    	return queueName;
    }

	public GetQueueUrlResult getQueueURL() {
		return queueURL;
	}

}

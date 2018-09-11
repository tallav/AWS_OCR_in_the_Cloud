package Manager;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import javax.jms.*;
import javax.jms.Message;

public class SQSClass {

	private AmazonSQS sqs;
	private String queueName;
	private GetQueueUrlResult queueURL;
	private AWSCredentialsProvider credentialsProvider;
	private String REGION;

	private SQSConnectionFactory connectionFactory;
	private SQSConnection connection;
	private Session session;
	private MessageConsumer consumer;
	private MessageProducer producer;
	
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
	/*
	public void startSQSConnection(){
		try {
			connection.start();
		} catch (JMSException e) {
			System.out.println("failed to start a SQSConnection");
		}
	}
	*/
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
			receivedMessage = consumer.receive(10000);
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

	public void deleteQueue(){
		System.out.println("Deleting a queue.\n");
		AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
		client.getAmazonSQSClient().deleteQueue(queueURL.getQueueUrl());
	}

	public void deleteMessage(String queueName,String receiptHandle){ //todo: fix, not working
		// Delete the message
		System.out.println("Deleting a message.\n");
		AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
		try {
			String queueUrl = client.getQueueUrl(queueName).getQueueUrl();
			connection.getWrappedAmazonSQSClient().deleteMessage(new DeleteMessageRequest(queueUrl,receiptHandle));
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	public String getQueueName(){
    	return queueName;
    }

	public GetQueueUrlResult getQueueURL() {
		return queueURL;
	}

    /*
    public AmazonSQS buildSqs(){
        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withRegion("us-east-1")
                .build();
        return sqs;
    }
    
    protected void createSqs(String queueName){
        System.out.println("Creating a new SQS queue");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
        sqs.createQueue(createQueueRequest);
    }
    
	protected List<Message> receiveMessagesFromQueue(){
        System.out.println("Receiving messages from MyQueue");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqs.getQueueUrl(queueName).getQueueUrl());
        return sqs.receiveMessage(receiveMessageRequest).getMessages();
    }
	
	protected void sendMsgToSqs(String msg){
        System.out.println("Sending a message to MyQueue");
        GetQueueUrlResult queueUrl = sqs.getQueueUrl(queueName);
        sqs.sendMessage(new SendMessageRequest(queueUrl.getQueueUrl(), msg));
        System.out.println("message: " +msg+ " was added succesfully to the queue");
    }

    protected boolean isQueueEmpty(){
	    String queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
        msgs = sqs.receiveMessage(
                new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(1)).getMessages();
        return msgs.size() == 0;
    }

	public List<Message> receiveMessage(ReceiveMessageRequest receiveMessageRequest) {
		return sqs.receiveMessage(receiveMessageRequest).getMessages();
	}
	
	public String getQueueUrl(){
		return sqs.getQueueUrl(queueName).getQueueUrl();
	}
	*/
}

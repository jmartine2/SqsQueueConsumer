package com.elephanigma.sqsqueueconsumer;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.apache.log4j.Logger;

import java.util.List;

import static java.lang.Thread.sleep;

public class QueueConsumer implements Runnable
{
	private org.apache.log4j.Logger log;
	private String queueUrl;
	private String queueRegion;
	private QueueMessageProcessor queueMessageProcessor;

	public interface QueueMessageProcessor
	{
		void processMessage(String message) throws MessageProcessingException;
	}

	public static class MessageProcessingException extends Exception
	{
		public MessageProcessingException(String message)
		{
			super(message);
		}

		public MessageProcessingException(String message, Throwable cause)
		{
			super(message, cause);
		}
	}

	public QueueConsumer(String queueUrl, String queueRegion, QueueMessageProcessor messageProcessor, Logger logger)
	{
		this.queueUrl = queueUrl;
		this.queueRegion = queueRegion;
		this.queueMessageProcessor = messageProcessor;
		log = logger;
	}

	@SuppressWarnings("InfiniteLoopStatement")
	public void run()
	{
		while(true)
		{
			Message message = getMessageFromQueue();
			if (message == null)
			{
				doSleep();
				continue;
			}

			final String messageBody = message.getBody();
			final String receiptHandle = message.getReceiptHandle();

			if (log != null)
				log.info("Received message " + receiptHandle);

			// DO WHATEVER NEEDS TO BE DONE WITH THE DATA
			try
			{
				queueMessageProcessor.processMessage(messageBody);
			}
			catch (MessageProcessingException e)
			{
				processBadMessage(receiptHandle,e.getMessage());
			}

			try {
				deleteMessageFromQueue(receiptHandle, queueUrl);
			} catch (Exception e) {
				if (log != null)
					log.error("Error deleting message from queue",e);
				e.printStackTrace();
			}

			doSleep();
		}
	}

	private void deleteMessageFromQueue(String receiptHandle, String queueUrl)
	{
		AmazonSQS sqs = getAmazonSQSClient();
		sqs.deleteMessage(new DeleteMessageRequest(queueUrl, receiptHandle));
	}

	private void processBadMessage(String receiptHandle, String reason)
	{
		if (receiptHandle != null) {
			if (log != null)
				log.error("Deleting bad message (" + reason + ")");
			deleteMessageFromQueue(receiptHandle, queueUrl);
			// TODO: Add a Dead Letter Queue
		}
	}

	private static void doSleep()
	{
		try {
			sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	protected Message getMessageFromQueue()
	{
		AmazonSQS sqs = getAmazonSQSClient();

		if (log != null)
			log.trace("Receiving messages from " + queueUrl);
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
		receiveMessageRequest.setMaxNumberOfMessages(1);

		List<Message> messages = null;
		try {
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		} catch (Exception e) {
			if (log != null)
				log.error("Unable to receive messages from " + queueUrl,e);
			return null;
		}

		if (messages.isEmpty()) {
			if (log != null)
				log.trace("No messages to receive.");
			return null;
		}

		return messages.get(0);
	}

	private AmazonSQS getAmazonSQSClient()
	{
		return AmazonSQSClientBuilder.standard()
				.withRegion(queueRegion)
				.withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
				.build();
	}

}

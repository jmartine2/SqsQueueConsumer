package com.elephanigma.sqsqueueconsumer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.util.Map;

public class QueueConsumerJson implements Runnable
{
	public void run()
	{
		Thread emailProcessingThread = new Thread(genericQueueConsumer);
		emailProcessingThread.start();
	}

	public interface JsonQueueMessageProcessor
	{
		void processMessage(Map<String,String> message) throws QueueConsumer.MessageProcessingException;
	}

	private JsonQueueMessageProcessor messageProcessor;
	private QueueConsumer genericQueueConsumer;

	public QueueConsumerJson(String queueUrl, String queueRegion, final JsonQueueMessageProcessor messageProcessor, Logger logger)
	{
		this.messageProcessor = messageProcessor;

		QueueConsumer.QueueMessageProcessor genericMessageProcessor = new QueueConsumer.QueueMessageProcessor()
		{
			public void processMessage(String message) throws QueueConsumer.MessageProcessingException
			{
				Map<String,String> messageMap = parseSqsJson(message);
				messageProcessor.processMessage(messageMap);
			}
		};

		genericQueueConsumer = new QueueConsumer(queueUrl, queueRegion, genericMessageProcessor, logger);

	}

	private static Map<String,String> parseFlatJson(String postData)
	{
		Gson gson = new Gson();
		Type type =	new TypeToken<Map<String, String>>(){}.getType();
		Map<String, String> map = gson.fromJson(postData, type);
		return map;
	}

	private static Map<String,String> parseSqsJson(String postData)
	{
		Gson gson = new Gson();
		Type type =	new TypeToken<Map<String, String>>(){}.getType();
		Map<String, String> sqsMap = gson.fromJson(postData, type);
		String message = sqsMap.get("Message");
		Map<String,String> messageMap = parseFlatJson(message);
		return messageMap;
	}
}

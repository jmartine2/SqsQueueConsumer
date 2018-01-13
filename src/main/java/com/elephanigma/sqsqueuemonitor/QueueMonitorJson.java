package com.elephanigma.sqsqueuemonitor;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.util.Map;

public class QueueMonitorJson implements Runnable
{
	public void run()
	{
		Thread emailProcessingThread = new Thread(genericQueueMonitor);
		emailProcessingThread.start();
	}

	public interface JsonQueueMessageProcessor
	{
		void processMessage(Map<String,String> message) throws QueueMonitor.MessageProcessingException;
	}

	private JsonQueueMessageProcessor messageProcessor;
	private QueueMonitor genericQueueMonitor;

	public QueueMonitorJson(String queueUrl, String queueRegion, final JsonQueueMessageProcessor messageProcessor, Logger logger)
	{
		this.messageProcessor = messageProcessor;

		QueueMonitor.QueueMessageProcessor genericMessageProcessor = new QueueMonitor.QueueMessageProcessor()
		{
			public void processMessage(String message) throws QueueMonitor.MessageProcessingException
			{
				Map<String,String> messageMap = parseSqsJson(message);
				messageProcessor.processMessage(messageMap);
			}
		};

		genericQueueMonitor = new QueueMonitor(queueUrl, queueRegion, genericMessageProcessor, logger);

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

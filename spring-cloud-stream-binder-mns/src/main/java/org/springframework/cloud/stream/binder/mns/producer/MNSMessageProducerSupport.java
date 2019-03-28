package org.springframework.cloud.stream.binder.mns.producer;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.endpoint.MessageProducerSupport;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.model.Message;

public class MNSMessageProducerSupport extends MessageProducerSupport {
	private static final Logger logger = LoggerFactory.getLogger(MNSMessageProducerSupport.class);
	public static final int WAIT_SECONDS = 30;
	private CloudQueue queue;

	public MNSMessageProducerSupport(CloudQueue queue) {
		this.queue = queue;
	}

	@Override
	protected void onInit() {
		super.onInit();
		receive();
	}

	private void receive() {
		while (true) {
			Message message = null;
			do {
				try {
					message = queue.popMessage(WAIT_SECONDS);
					String handle = message.getReceiptHandle();
					createAndSend(message);
					queue.deleteMessage(handle);
				} catch (Exception e) {
					logger.warn("Pop messag error for queue " + queue.getAttributes().getQueueName());
				}
			} while (message == null);
		}
	}

	private void createAndSend(Message msg) {
		String body = msg.getMessageBodyAsString();
		Map<String, Object> headers = new HashMap<>();
		headers.put("RequestId", msg.getRequestId());
		headers.put("Queue", queue.getAttributes().getQueueName());
		org.springframework.messaging.Message<String> messagingMessage = getMessageBuilderFactory().withPayload(body)
				.copyHeaders(headers).build();
		sendMessage(messagingMessage);
	}

	public CloudQueue getQueue() {
		return queue;
	}

	public void setQueue(CloudQueue queue) {
		this.queue = queue;
	}
}

package org.springframework.cloud.stream.binder.mns.handler;

import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import com.aliyun.mns.client.CloudTopic;
import com.aliyun.mns.model.Base64TopicMessage;
import com.aliyun.mns.model.TopicMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MNSMessageProducingMessageHandler extends AbstractReplyProducingMessageHandler {
	private CloudTopic topic;
	private ObjectMapper jsonObjectMapper;

	public MNSMessageProducingMessageHandler(CloudTopic topic) {
		this.topic = topic;
	}

	@Override
	protected Object handleRequestMessage(Message<?> requestMessage) {
		MessageHeaders headers = requestMessage.getHeaders();
		Object payload = requestMessage.getPayload();
		TopicMessage msg = new Base64TopicMessage();
		if (headers.containsKey(BinderHeaders.PARTITION_HEADER)) {
			int partition = (int) headers.get(BinderHeaders.PARTITION_HEADER);
			msg.setMessageTag(BinderHeaders.PARTITION_HEADER + "_" + partition);
		}
		if (payload == null) {
			return null;
		}
		if (payload instanceof byte[]) {
			msg.setMessageBody((byte[]) payload);
		} else if (payload instanceof String) {
			msg.setMessageBody((String) payload);
		} else {
			try {
				msg.setMessageBody(jsonObjectMapper.writeValueAsString(payload));
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		topic.publishMessage(msg);
		return null;
	}

	public CloudTopic getTopic() {
		return topic;
	}

	public void setTopic(CloudTopic topic) {
		this.topic = topic;
	}

	public ObjectMapper getJsonObjectMapper() {
		return jsonObjectMapper;
	}

	public void setJsonObjectMapper(ObjectMapper jsonObjectMapper) {
		this.jsonObjectMapper = jsonObjectMapper;
	}

}

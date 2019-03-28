package org.springframework.cloud.stream.binder.mns;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.mns.handler.MNSMessageProducingMessageHandler;
import org.springframework.cloud.stream.binder.mns.producer.MNSMessageProducerSupport;
import org.springframework.cloud.stream.binder.mns.provisioning.MNSTopicQueueProvisioner;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.util.Assert;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.CloudTopic;
import com.aliyun.mns.client.MNSClient;

import shaded.org.apache.commons.lang3.StringUtils;

public class MNSMessageChannelBinder
		extends AbstractMessageChannelBinder<ConsumerProperties, ProducerProperties, MNSTopicQueueProvisioner> {

	@Autowired
	private BeanFactory beanFactory;
	private MNSClient client;

	public MNSMessageChannelBinder(MNSClient client, MNSTopicQueueProvisioner provisioningProvider) {
		super(new String[0], provisioningProvider);
		this.client = client;
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			ProducerProperties producerProperties, MessageChannel errorChannel) throws Exception {
		String topicName = destination.getName();
		CloudTopic topic = client.getTopicRef(topicName);
		Assert.notNull(topic, "Destination " + topicName + " not exists.");
		MNSMessageProducingMessageHandler handler = new MNSMessageProducingMessageHandler(topic);
		handler.setBeanFactory(beanFactory);
		return handler;
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
			ConsumerProperties properties) throws Exception {
		String topicName = destination.getName();
		String queueName = topicName + "-receiver" + getSuffix(group);
		if (properties.isPartitioned()) {
			queueName += "-" + properties.getInstanceIndex();
		}
		CloudQueue queue = client.getQueueRef(queueName);
		MNSMessageProducerSupport messageProducer = new MNSMessageProducerSupport(queue);
		messageProducer.setBeanFactory(getBeanFactory());
		return messageProducer;
	}
	
	private String getSuffix(String group) {
		if (StringUtils.isBlank(group)) {
			return "";
		}
		return "-" + group;
	}

}

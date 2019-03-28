package org.springframework.cloud.stream.binder.mns.provisioning;

import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.CloudTopic;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.model.QueueMeta;
import com.aliyun.mns.model.SubscriptionMeta;
import com.aliyun.mns.model.SubscriptionMeta.NotifyContentFormat;
import com.aliyun.mns.model.TopicMeta;

import shaded.org.apache.commons.lang3.StringUtils;

public class MNSTopicQueueProvisioner implements ProvisioningProvider<ConsumerProperties, ProducerProperties> {

	private MNSClient client;

	public MNSTopicQueueProvisioner(MNSClient client) {
		this.client = client;
	}

	@Override
	public ProducerDestination provisionProducerDestination(String name, ProducerProperties properties)
			throws ProvisioningException {
		String topicName = name;
		CloudTopic topic = declareTopic(name);
		for (String requiredGroupName : properties.getRequiredGroups()) {
			if (!properties.isPartitioned()) {
				String queueName = topicName + "-receiver-" + requiredGroupName;
				CloudQueue queue = declareQueue(queueName);
				notPartitionedBinding(topic, queue);
			} else {
				for (int i = 0; i < properties.getPartitionCount(); i++) {
					String partitionSuffix = "-" + i;
					String partitionQueueName = topicName + "-receiver-" + requiredGroupName + partitionSuffix;
					CloudQueue queue = declareQueue(partitionQueueName);
					partitionedBinding(topic, queue, i);
				}
			}
		}
		return new MNSProducerDestination(topic);
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group, ConsumerProperties properties)
			throws ProvisioningException {
		String topicName = name;
		CloudTopic topic = declareTopic(name);
		String queueName = topicName + "-receiver" + getSuffix(group);
		if (!properties.isPartitioned()) {
			CloudQueue queue = declareQueue(queueName);
			notPartitionedBinding(topic, queue);
		} else {
			queueName += "-" + properties.getInstanceIndex();
			CloudQueue queue = declareQueue(queueName);
			partitionedBinding(topic, queue, properties.getInstanceIndex());
		}

		return new MNSConsumerDestination(topic);
	}

	private CloudTopic declareTopic(String topicName) {
		TopicMeta meta = new TopicMeta();
		meta.setTopicName(topicName);
		CloudTopic topic = client.createTopic(meta);
		return topic;
	}

	private CloudQueue declareQueue(String queueName) {
		CloudQueue queue = client.getQueueRef(queueName);
		QueueMeta queueMeta = new QueueMeta();
		queueMeta.setPollingWaitSeconds(5);
		queueMeta.setQueueName(queueName);
		queue = client.createQueue(queueMeta);
		return queue;
	}

	private SubscriptionMeta notPartitionedBinding(CloudTopic topic, CloudQueue queue) {
		String queueName = queue.getAttributes().getQueueName();
		String subName = "sub-for-queue-" + queueName;
		String queueEndpoint = topic.generateQueueEndpoint(queueName);
		SubscriptionMeta subMeta = new SubscriptionMeta();
		subMeta.setSubscriptionName(subName);
		subMeta.setNotifyContentFormat(NotifyContentFormat.SIMPLIFIED);
		subMeta.setNotifyStrategy(SubscriptionMeta.NotifyStrategy.EXPONENTIAL_DECAY_RETRY);
		subMeta.setEndpoint(queueEndpoint);
		topic.subscribe(subMeta);
		return subMeta;
	}

	private SubscriptionMeta partitionedBinding(CloudTopic topic, CloudQueue queue, int index) {
		String queueName = queue.getAttributes().getQueueName();
		String subName = "sub-for-queue-" + queueName;
		String queueEndpoint = topic.generateQueueEndpoint(queueName);
		SubscriptionMeta subMeta = new SubscriptionMeta();
		subMeta.setSubscriptionName(subName);
		subMeta.setNotifyContentFormat(NotifyContentFormat.SIMPLIFIED);
		subMeta.setNotifyStrategy(SubscriptionMeta.NotifyStrategy.EXPONENTIAL_DECAY_RETRY);
		subMeta.setEndpoint(queueEndpoint);
		subMeta.setFilterTag(BinderHeaders.PARTITION_HEADER + "_" + index);
		topic.subscribe(subMeta);
		return subMeta;
	}

	private String getSuffix(String group) {
		if (StringUtils.isBlank(group)) {
			return "";
		}
		return "-" + group;
	}

	private static final class MNSProducerDestination implements ProducerDestination {
		private CloudTopic topic;

		public MNSProducerDestination(CloudTopic topic) {
			this.topic = topic;
		}

		@Override
		public String getName() {
			return topic.getAttribute().getTopicName();
		}

		@Override
		public String getNameForPartition(int partition) {
			return topic.getAttribute().getTopicName();
		}

	}

	private static final class MNSConsumerDestination implements ConsumerDestination {
		private CloudTopic topic;

		public MNSConsumerDestination(CloudTopic topic) {
			this.topic = topic;
		}

		@Override
		public String getName() {
			return topic.getAttribute().getTopicName();
		}

	}

}

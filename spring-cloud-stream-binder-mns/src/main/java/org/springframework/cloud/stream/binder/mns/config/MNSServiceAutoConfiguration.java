package org.springframework.cloud.stream.binder.mns.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.mns.MNSMessageChannelBinder;
import org.springframework.cloud.stream.binder.mns.provisioning.MNSTopicQueueProvisioner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.aliyun.mns.client.CloudAccount;
import com.aliyun.mns.client.MNSClient;

@Configuration
@EnableConfigurationProperties(MNSProperties.class)
public class MNSServiceAutoConfiguration {

	@Autowired
	public MNSProperties mnsProperties;

	@Bean
	public MNSClient client(MNSProperties mnsProperties) {
		CloudAccount account = new CloudAccount(mnsProperties.getAccessId(), mnsProperties.getAccessKey(),
				mnsProperties.getEndpoint());
		return account.getMNSClient();
	}

	@Bean
	public MNSTopicQueueProvisioner mnsTopicQueueProvisioner(MNSClient client) {
		return new MNSTopicQueueProvisioner(client);
	}

	@Bean
	public MNSMessageChannelBinder mnsMessageChannelBinder(MNSClient client,
			MNSTopicQueueProvisioner mnsTopicQueueProvisioner) {
		return new MNSMessageChannelBinder(client, mnsTopicQueueProvisioner);
	}

}

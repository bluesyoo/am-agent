package kr.co.admonster.agent.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import kr.co.admonster.agent.service.BiddingService;
import kr.co.admonster.kafka.domain.BiddingTaskMessage;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BiddingListener {
	
	private final BiddingService biddingService;
	
	public BiddingListener(BiddingService biddingService) {
		this.biddingService = biddingService;
	}
	
	@KafkaListener(
			id = "bidding-listener",
			topics = "#{@kafkaTopicProvider.BIDDING}",
			groupId = "am-agent",
			containerFactory = "kafkaListenerContainerFactory")
	public void onMessage(ConsumerRecord<String, BiddingTaskMessage> record, Acknowledgment ack) {
		log.info("Collect message: {}", record.value());
		try {
			this.biddingService.run(record.value());
			ack.acknowledge();
		} catch (Exception e) {
			log.error("Failed to process task {}. {}", record.value(), e.getMessage(), e);
		}
	}
	
}

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
		String key = record.key();
		BiddingTaskMessage value = record.value();
		
		int partition = record.partition();
		long offset = record.offset();
		
		log.info("Received message key={} partition={} offset={} value={}", key, partition, offset, value);
		
		try {
			this.biddingService.run(record.value());
			ack.acknowledge();
			
			log.info("Successfully processed keywordId={} offset={}", value.getKeywordId(), offset);
		} catch (Exception e) {
			log.error("Failed to process message key={} offset={}. Error={}", key, offset, e.getMessage(), e);
		}
	}
	
}

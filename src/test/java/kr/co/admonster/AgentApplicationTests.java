package kr.co.admonster;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import kr.co.admonster.common.constants.enums.BiddingType;
import kr.co.admonster.common.constants.enums.CampaignType;
import kr.co.admonster.common.constants.enums.DeviceType;
import kr.co.admonster.common.dto.PidGains;
import kr.co.admonster.kafka.domain.BiddingTaskMessage;
import kr.co.admonster.kafka.naming.MessageType;
import kr.co.admonster.kafka.producer.MessageProducer;

@SpringBootTest
class AgentApplicationTests {
	
	@Autowired
	private MessageProducer messageProducer;
	
	/**
	 * private String keywordId;
	 * private String keyword;
	 * private String displayUrl;
	 * 
	 * private DeviceType deviceType;
	 * private CampaignType campaignType;
	 * private biddingType biddingType;
	 * 
	 * private Integer targetRank;         // 목표 순위
	 * private Double currentBid;          // 현재 입찰가
	 * private Double minimumBid;
	 * private Double maximumBid;
	 * 
	 * private Double previousError;
	 * private Double integralError;
	 * 
	 * private String accountNo;
	 * private String accessLicense;
	 * private String secretKey;
	 * 
	 * private Integer pidClusterId;
	 * private PidGains pidGains;
	 */
	@Test
	void contextLoads() throws Exception {
		BiddingTaskMessage taskMessage = BiddingTaskMessage.builder()
				.messageType(MessageType.BIDDING)
				
				.keywordId(UUID.randomUUID().toString())
				.keyword("평택포장이사")
				.displayUrl("m.mcygclean.com")
				
				.deviceType(DeviceType.PC)
				.campaignType(CampaignType.WEB_SITE)
				.biddingType(BiddingType.AI)
				
				.targetRank(3)
				.currentBid(160D)
				.minimumBid(70D)
				.maximumBid(200D)
				
				.previousError(0D)
				.integralError(0D)
				
				.accountNo("123456789")
				.accessLicense("")
				.secretKey("")
				
				.pidClusterId(1)
				.pidGains(new PidGains(0D, 0D, 0D, 1D))
				.build();
		
		this.messageProducer.send(taskMessage);
	}
	
}

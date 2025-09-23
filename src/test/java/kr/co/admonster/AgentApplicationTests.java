package kr.co.admonster;

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
	 * // 입찰 상품 정보
	 * private BiddingType biddingType;
	 * private Double maxBid;
	 * 
	 * // 입찰 실행 정보
	 * private Integer targetRank;         // 목표 순위
	 * private Double currentBid;          // 현재 입찰가
	 * private Double presetBid;           // 설정 입찰가
	 * 
	 * private Double previousError;
	 * private Double integralError;
	 * 
	 * private PidGains pidGains;
	 * 
	 * // api 정보
	 * private String customerId;
	 * private String accessLicense;
	 * private String secretKey;
	 */
	@Test
	void contextLoads() throws Exception {
		BiddingTaskMessage taskMessage = BiddingTaskMessage.builder()
				.messageType(MessageType.BIDDING)
				.keyword("평택포장이사")
				.displayUrl("m.mcygclean.com")
				
				.deviceType(DeviceType.PC)
				.campaignType(CampaignType.WEB_SITE)
				.biddingType(BiddingType.AI)
				.minBid(70D)
				.maxBid(200D)
				
				.targetRank(3)
				.currentBid(160D)
				.presetBid(0D)
				
				.previousError(0D)
				.integralError(0D)
				
				.pidClusterId(1)
				.pidGains(new PidGains(0D, 0D, 0D, 1D))
				
				.accountNo(1L)
				.accessLicense("")
				.secretKey("444")
				.build();
		
		this.messageProducer.send(taskMessage);
	}
	
}

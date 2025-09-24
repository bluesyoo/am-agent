package kr.co.admonster.agent.service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;

import kr.co.admonster.agent.client.NaverSearchadClient;
import kr.co.admonster.common.constants.enums.ResultState;
import kr.co.admonster.common.constants.enums.TimestampType;
import kr.co.admonster.common.utility.SignatureGenerator;
import kr.co.admonster.kafka.domain.BiddingResultMessage;
import kr.co.admonster.kafka.domain.BiddingTaskMessage;
import kr.co.admonster.kafka.producer.MessageProducer;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class BiddingService {
	
	private final MessageProducer messageProducer;
	
	private final NaverRankCrawler naverRankCrawler;
	private final PidCalculator pidCalculator;
	
	private final NaverSearchadClient naverSearchadClient;
	
	public BiddingService(
			MessageProducer messageProducer,
			NaverRankCrawler naverRankCrawler,
			PidCalculator pidCalculator,
			NaverSearchadClient naverSearchadClient) {
		this.messageProducer = messageProducer;
		
		this.naverRankCrawler = naverRankCrawler;
		this.pidCalculator = pidCalculator;
		
		this.naverSearchadClient = naverSearchadClient;
	}
	
	public void run(BiddingTaskMessage taskMessage) {
		String keywordId = taskMessage.getKeywordId();
		log.info("Start bidding_task. keyword_id={}", keywordId);
		
		// Step 1: Create a BiddingResultMessage from the incoming task message.
		BiddingResultMessage resultMessage = BiddingResultMessage.from(taskMessage);
		LinkedHashMap<String, Long> timestamps = resultMessage.getTimestamps();
		
		try {
			timestamps.put(TimestampType.TOTAL_START.getValue(), System.currentTimeMillis());
			
			process(taskMessage, resultMessage, timestamps);
			update(taskMessage, resultMessage, timestamps);
			
			timestamps.put(TimestampType.TOTAL_END.getValue(), System.currentTimeMillis());
			resultMessage.setResultState(ResultState.SUCCESS);
			
			log.info("Completed bidding_task successfully. keyword_id={}", keywordId);
		} catch (Exception e) {
			log.error("Failed to run bidding_task. keyword_id={}", keywordId, e);
			
			timestamps.put(TimestampType.TOTAL_END.getValue(), System.currentTimeMillis());
			resultMessage.setResultState(ResultState.FAILED);
		} finally {
			this.messageProducer.send(resultMessage);
		}
	}
	
	private void process(BiddingTaskMessage taskMessage, BiddingResultMessage resultMessage, Map<String, Long> timestamps) throws Exception {
		String keywordId = taskMessage.getKeywordId();
		log.info("Start bidding process. keyword_id={}", keywordId);
		
		double finalPrice = 0D;
		
		switch (taskMessage.getBiddingType()) {
		case AI:
		case SMART:
			// Step 2: Pass the resultMessage to the crawler.
			// The crawler will update this same object with the rank.
			timestamps.put(TimestampType.CRAWLER_START.getValue(), System.currentTimeMillis());
			this.naverRankCrawler.getRank(resultMessage);
			timestamps.put(TimestampType.CRAWLER_END.getValue(), System.currentTimeMillis());
			
			// Step 3: Pass the updated resultMessage to the calculator.
			// The calculator will add the new bid and PID state.
			timestamps.put(TimestampType.CALCULATOR_START.getValue(), System.currentTimeMillis());
			this.pidCalculator.calculate(resultMessage);
			timestamps.put(TimestampType.CALCULATOR_END.getValue(), System.currentTimeMillis());
			
			finalPrice = resultMessage.getFinalPrice();
			break;
			
		case SCHEDULED:
			// 크롤링과 PID 계산을 생략하고, 미리 설정된 입찰가를 사용
			log.info("Scheduled bidding detected. Skipping crawl and PID calculation. keyword_id={}", keywordId);
			
			finalPrice = taskMessage.getCurrentBid(); // bidding_task 생성시 설정
			break;
			
		case ESTIMATED:
			// 매체사 API를 통해 예상 입찰가 조회
			log.info("Estimated bidding detected. Calling estimate API. keyword_id={}", keywordId);
			
			timestamps.put("estimate_api_start", System.currentTimeMillis());
			// getEstimatedBid 메서드는 예상 입찰가를 반환해야 합니다.
//			finalPrice = apiClient.getEstimatedBid(taskMessage.getKeyword(), taskMessage.getAccessLicense(), taskMessage.getSecretKey());
			timestamps.put("estimate_api_end", System.currentTimeMillis());
			break;
			
		case PREDICTIVE:
			// 예측 모델을 통해 입찰가 산정 (시간 측정)
			log.info("Predictive bidding detected. Running predictive model. keyword_id={}", keywordId);
			
			timestamps.put("predictive_start", System.currentTimeMillis());
//			finalPrice = getPredictedBid(taskMessage);
			timestamps.put("predictive_end", System.currentTimeMillis());
			
		default:
			log.error("Unknown bidding type. keyword_id={} type={}", keywordId, taskMessage.getBiddingType());
			
			throw new IllegalArgumentException("Invalid bidding type.");
		}
		
		finalPrice = Math.max(Math.min(finalPrice, taskMessage.getMaximumBid()), taskMessage.getMinimumBid());
		resultMessage.setFinalPrice(finalPrice);
		
		log.info("Bidding process completed. keyword_id={} final_price={}", keywordId, finalPrice);
	}

	
	// Step 4: Call the API with the now complete resultMessage.
	// The method should be apiClient.updateBid(resultMessage).
	private void update(BiddingTaskMessage taskMessage, BiddingResultMessage resultMessage, Map<String, Long> timestamps) throws Exception {
		// ############## 개발 코드 #################
		if (taskMessage.getAccessLicense().length() == 0) {
			return;
		}

		String keywordId = taskMessage.getKeywordId();
		log.info("Start update API call. keyword_id={}", keywordId);
		
		timestamps.put(TimestampType.API_CALL_START.getValue(), System.currentTimeMillis());
		
		// 1. 요청에 필요한 데이터 준비
		String accountNo = taskMessage.getAccountNo();
		String accessLicense = taskMessage.getAccessLicense();
		String secretKey = taskMessage.getSecretKey();
		
		Double finalPrice = resultMessage.getFinalPrice();
		
		String timestamp = String.valueOf(Instant.now().getEpochSecond());
		String method = HttpMethod.PUT.name();
		String requestUri = "/ncc/keywords/" + keywordId;
		
		// 2. 시그니처 생성
		String signature = SignatureGenerator.generate(
				timestamp,
				method,
				requestUri,
				secretKey);
		
		// 3. Feign Client 호출 (헤더 값들을 직접 전달)
		this.naverSearchadClient.updateBid(
				accessLicense,
				accountNo,
				timestamp,
				signature,
				keywordId,
				finalPrice.longValue());
		
		timestamps.put(TimestampType.API_CALL_END.getValue(), System.currentTimeMillis());
		
		log.info("Completed update API call successfully. keyword_id={} final_price={}", keywordId, finalPrice);
	}
	
}

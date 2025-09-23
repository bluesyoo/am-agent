package kr.co.admonster.agent.client;

import java.util.Map;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "naver-searchad-api", url = "https://api.naver.com")
public interface NaverSearchadClient {
	
	/**
	 * 입찰가를 갱신하는 API입니다.
	 * @param apiKey X-API-KEY 헤더
	 * @param customerId X-CUSTOMER 헤더
	 * @param timestamp X-Timestamp 헤더
	 * @param signature X-Signature 헤더
	 * @param keywordId 갱신할 키워드 ID
	 * @param requestBody 입찰가를 담은 DTO 리스트
	 */
	@PutMapping("/ncc/keywords/{keywordId}")
	void updateBid(
		@RequestHeader("X-API-KEY") String apiKey,
		@RequestHeader("X-CUSTOMER") String customerId,
		@RequestHeader("X-Timestamp") String timestamp,
		@RequestHeader("X-Signature") String signature,
		@RequestBody Map<String, Object> requestBody
	);

	/**
	 * 키워드의 예상 입찰가를 조회하는 API입니다.
	 * @param apiKey X-API-KEY 헤더
	 * @param customerId X-CUSTOMER 헤더
	 * @param timestamp X-Timestamp 헤더
	 * @param signature X-Signature 헤더
	 * @param keywordId 조회할 키워드 ID
	 */
	@GetMapping("/searchad-api/v1/keyword/{keywordId}/estimates/bids")
	Map<String, Object> getEstimatedBid(
		@RequestHeader("X-API-KEY") String apiKey,
		@RequestHeader("X-CUSTOMER") String customerId,
		@RequestHeader("X-Timestamp") String timestamp,
		@RequestHeader("X-Signature") String signature,
		@RequestParam String keywordId
	);
	
}

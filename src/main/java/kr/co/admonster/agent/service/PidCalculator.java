package kr.co.admonster.agent.service;

import org.springframework.stereotype.Component;

import kr.co.admonster.common.dto.PidGains;
import kr.co.admonster.kafka.domain.BiddingResultMessage;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class PidCalculator {
	
	/**
	 * 입찰 작업 메시지를 기반으로 새로운 입찰가를 계산합니다.
	 * @param message 입찰에 필요한 모든 정보를 담고 있는 BiddingTaskMessage 객체
	 */
	public void calculate(BiddingResultMessage message) {
		String keywordId = message.getKeywordId();
		double currentBid = message.getCurrentBid();
		
		log.info("Start PID calculation. keyword_id={}", keywordId);
		
		double finalPrice;
		
		// PID 계산에 필요한 현재 상태 변수 가져오기
		int targetRank = message.getTargetRank();
		int viewedRank = message.getViewedRank();
		
		double previousError = message.getUpdatedPreviousError();
		double integralError = message.getUpdatedIntegralError();
		
		// PID 게인 값 가져오기
		PidGains pidGains = message.getPidGains();
		double kp = pidGains.getKp();
		double ki = pidGains.getKi();
		double kd = pidGains.getEffectiveGainD();
		
		if (viewedRank == -1) {
			// --- 1. 순위가 -1인 경우 특별 처리 ---
			log.warn("Keyword not found in rank. Applying special bid increase. keyword_id={}", keywordId);
			finalPrice = currentBid + 100.0; // 고정된 값만큼 인상
			
			// 적분 오차를 0으로 초기화하여 입찰가 폭등 방지
			message.setUpdatedPreviousError(0.0);
			message.setUpdatedIntegralError(0.0);
		} else {
			// --- 2. 순위가 존재하는 경우 PID 공식 적용 ---
			double error = (double)targetRank - viewedRank;
			log.debug("error={} target_rank={} viewed_rank={}", error, targetRank, viewedRank);
			
			double newIntegralError = integralError + error;
			log.debug("new_integral_error={} integral_error={} error={}", newIntegralError, integralError, error);
			
			double derivative = error - previousError;
			log.debug("derivative={} error={} previous_error={}", derivative, error, previousError);
			
			finalPrice = currentBid + (kp * error) + (ki * newIntegralError) + (kd * derivative);
			
			// 계산 결과를 메시지 객체에 저장
			message.setUpdatedPreviousError(error);
			message.setUpdatedIntegralError(newIntegralError);
		}
		
		finalPrice = Math.round(finalPrice / 10) * 10;
		
		// 계산 결과를 메시지 객체에 다시 저장
		message.setFinalPrice(finalPrice);
		
		log.info("Completed PID calculation successfully. keyword_id={} current_bid={} final_price={}", keywordId, currentBid, finalPrice);
	}
	
}

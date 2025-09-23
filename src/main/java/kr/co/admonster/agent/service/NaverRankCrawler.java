package kr.co.admonster.agent.service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;

import kr.co.admonster.common.constants.enums.CampaignType;
import kr.co.admonster.common.constants.enums.DeviceType;
import kr.co.admonster.kafka.domain.BiddingResultMessage;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class NaverRankCrawler {
	
	private final String powerlinkP = "https://search.naver.com/search.naver?query=";
	private final String powerlinkM = "https://m.search.naver.com/search.naver?query=";
	
	private final String shoppingP = "https://search.shopping.naver.com/search/all?query=";
	private final String shoppingM = "https://msearch.shopping.naver.com/search/all?query=";
	
	public void getRank(BiddingResultMessage message) throws Exception {
		CampaignType campaignType = message.getCampaignType();
		switch (campaignType) {
		case WEB_SITE:
			getPowerlinkRank(message);
			break;
		case SHOPPING:
			getShoppingRank(message);
			break;
		default:
			break;
		}
	}
	
	/**
	 * 네이버 검색 페이지에서 키워드 순위를 조회합니다.
	 * @param message 입찰 작업에 필요한 정보를 담은 BiddingTaskMessage 객체
	 * @throws Exception 크롤링 과정에서 오류가 발생했을 때
	 */
	private void getPowerlinkRank(BiddingResultMessage message) throws Exception {
		DeviceType deviceType = message.getDeviceType();
		
		String keyword = message.getKeyword();
		String displayUrl = message.getDisplayUrl();
		log.info("Starting powerlink rank search for keyword: '{}', displayUrl: '{}' [{}]", keyword, displayUrl, deviceType);
		
		try (Playwright playwright = Playwright.create()) {
			String searchUrl = null;
			String selectorAdSection = null;
			String selectorAdElement = null;
			String selectorDisplayUrl = null;
			
			if (deviceType == DeviceType.MOBILE) {
				searchUrl = this.powerlinkM;
				selectorAdSection = "ul#power_link_body";
				selectorAdElement = "li.bx.img_type";
				selectorDisplayUrl = "div.url_inner span.url";
			} else {
				searchUrl = this.powerlinkP;
				selectorAdSection = "div#power_link_body";
				selectorAdElement = "li.lst.js-hover-item";
				selectorDisplayUrl = "span.lnk_url_area a.lnk_url";
			}
			
			// 브라우저 실행 (헤드리스 모드)
			Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(true));
			Page page = browser.newPage();
			
			// 네이버 검색 페이지로 이동
			page.navigate(searchUrl + keyword);
			log.debug("Navigated to powerlink search results page for keyword: {}", keyword);
			
			// 메인 광고 섹션이 존재할 때까지 최대 15초 대기
			Locator adSection = page.locator(selectorAdSection);
			adSection.waitFor(new Locator.WaitForOptions().setTimeout(15000));
			log.info("Powerlink ad section found.");
			
			if (adSection.isVisible()) {
				// 광고 섹션 내의 모든 광고 목록을 추출
				// m: li.bx.img_type
				// p: li.lst.js-hover-item
				Locator adElements = adSection.locator(selectorAdElement);
				
				// 최소 하나의 광고가 나타날 때까지 최대 10초 대기
				adElements.first().waitFor(new Locator.WaitForOptions().setTimeout(10000));
				int total = adElements.count();
				log.info("Found {} powerlink ad elements.", total);
				
				LinkedHashMap<String, Integer> adRanks = new LinkedHashMap<>();
				
				// 각 광고 아이템을 순회하며 목표 URL과 일치하는 광고를 찾음
				for (int inx = 0; inx < total; inx++) {
					Locator currentAd = adElements.nth(inx);
					
					// 'lnk_url' 클래스를 가진 요소의 텍스트를 추출
					// 이 부분은 제공해주신 HTML 구조에 기반한 선택자입니다.
					// m: div.url_inner span.url
					// p: span.lnk_url_area a.lnk_url
					String displayUrlText = currentAd.locator(selectorDisplayUrl).textContent().trim();
					
					adRanks.put(displayUrlText, inx + 1);
					log.debug("Powerlink ad found: {} at rank {}", displayUrlText, inx + 1);
				}
				
				// 최종 순위를 메시지 객체에 설정
				int clientRank = Optional.ofNullable(adRanks.get(message.getDisplayUrl()))
						.orElse(-1);
				
				message.setCurrentRank(clientRank);
				message.setCompetitorRanks(adRanks);
				log.info("Powerlink ad final rank result: Client's ad rank is {}. Found {} ads.", clientRank, adRanks.size());
			} else {
				log.warn("Powerlink ad section does not exist for keyword '{}'.", keyword);
				message.setCurrentRank(-1);
			}
		} catch (Exception e) {
			log.error("Powerlink crawling failed for keyword '{}' with error: {}", keyword, e.getMessage(), e);
			// 예외 발생 시 메시지 객체의 순위를 -1로 설정하고 예외를 다시 던짐
			message.setCurrentRank(-1);
			throw e;
		}
	}
	
	private void getShoppingRank(BiddingResultMessage message) throws Exception {
		DeviceType deviceType = message.getDeviceType();
		
		String keyword = message.getKeyword();
		String displayUrl = message.getDisplayUrl();
		log.info("Starting shopping rank search for keyword: '{}', displayUrl: '{}' [{}]", keyword, displayUrl, deviceType);
		
		try (Playwright playwright = Playwright.create()) {
			String searchUrl = null;
			String selectorAdSection = null;
			String selectorAdElement = null;
			String selectorDisplayUrl = null;
			
			if (deviceType == DeviceType.MOBILE) {
				searchUrl = this.shoppingM;
				selectorAdSection = "ul[class^='list_basis__']";
				selectorAdElement = "li[class^='adProduct_item__']";
				selectorDisplayUrl = "div[class^='url_inner__'] span[class^='url__']";
			} else {
				searchUrl = this.shoppingP;
				selectorAdSection = "div[class^='basicList_list_basis__']";
				selectorAdElement = "div[class^='adProduct_item__']";
				selectorDisplayUrl = "span[class^='adProduct_url__'] a, span[class^='adProduct_url__'] span";
			}
			
			Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(true));
			Page page = browser.newPage();
			
			// 네이버 쇼핑 검색 페이지로 이동
			page.navigate(searchUrl + keyword);
			log.debug("Navigated to shopping search results page for keyword: {}", keyword);
			
			// 광고 섹션 선택
			Locator adSection = page.locator(selectorAdSection);
			adSection.waitFor(new Locator.WaitForOptions().setTimeout(15000));
			log.info("Shopping ad section found.");
			
			if (adSection.isVisible()) {
				// 광고 아이템 목록
				Locator adElements = adSection.locator(selectorAdElement);
				
				adElements.first().waitFor(new Locator.WaitForOptions().setTimeout(10000));
				int total = adElements.count();
				log.info("Found {} shopping ad elements.", total);
				
				LinkedHashMap<String, Integer> adRanks = new LinkedHashMap<>();
				
				for (int inx = 0; inx < total; inx++) {
					Locator currentAd = adElements.nth(inx);
					
					// URL 위치: 모바일 / PC 분기
					String displayUrlText = currentAd.locator(selectorDisplayUrl).textContent().trim();
					
					// 광고의 순위를 맵에 저장
					adRanks.put(displayUrlText, inx + 1);
					log.debug("Shopping ad found: {} at rank {}", displayUrlText, inx + 1);
				}
				
				// 최종 순위를 메시지 객체에 설정
				int clientRank = Optional.ofNullable(adRanks.get(message.getDisplayUrl()))
						.orElse(-1);
				
				message.setCurrentRank(clientRank);
				message.setCompetitorRanks(adRanks);
				log.info("Shopping ad final rank result: Client's ad rank is {}. Found {} ads.", clientRank, adRanks.size());
			} else {
				log.warn("Shopping ad section does not exist for keyword '{}'.", keyword);
				message.setCurrentRank(-1);
			}
		} catch (Exception e) {
			log.error("Shopping crawling failed for keyword '{}' with error: {}", keyword, e.getMessage(), e);
			message.setCurrentRank(-1);
			throw e;
		}
	}
	
}

package kr.co.admonster.agent.service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;

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
	
	private final List<String> userAgents = List.of(
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
			"Mozilla/5.0 (Linux; Android 13; Pixel 6) AppleWebKit/537.36 Chrome/118.0.5993.117 Mobile Safari/537.36"
		);
	
	public void getRank(BiddingResultMessage message) throws Exception {
		CampaignType campaignType = message.getCampaignType();
		switch (campaignType) {
		case WEB_SITE -> getPowerlinkRank(message);
		case SHOPPING -> getShoppingRank(message);
		default -> log.warn("Unsupported campaign type. campaign_type={} keyword_id={}", campaignType, message.getKeywordId());
		}
	}
	
	/**
	 * 네이버 검색 페이지에서 키워드 순위를 조회합니다.
	 * @param message 입찰 작업에 필요한 정보를 담은 BiddingTaskMessage 객체
	 * @throws Exception 크롤링 과정에서 오류가 발생했을 때
	 */
	private void getPowerlinkRank(BiddingResultMessage message) throws Exception {
		Random random = new Random();
		int delay = 1000 + random.nextInt(2000); // 1~3초 대기
		Thread.sleep(delay);
		
		String keywordId = message.getKeywordId();
		String keyword = message.getKeyword();
		String displayUrl = message.getDisplayUrl();
		DeviceType deviceType = message.getDeviceType();
		
		log.info("Start powerlink rank search. keyword_id={} keyword='{}' display_url={} device={} delay={}", keywordId, keyword, displayUrl, deviceType, delay);
		
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
//			Page page = browser.newPage();
			
//			Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(false));
			
			Page page = browser.newPage(new Browser.NewPageOptions()
					.setUserAgent(this.userAgents.get(random.nextInt(this.userAgents.size()))));
//					.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"));
			
			// 네이버 검색 페이지로 이동
			page.navigate(searchUrl + keyword);
			log.debug("Navigated to powerlink search results. keyword_id={} url={}", keywordId, searchUrl + keyword);
			
			// 메인 광고 섹션이 존재할 때까지 최대 15초 대기
			Locator adSection = page.locator(selectorAdSection);
			adSection.waitFor(new Locator.WaitForOptions().setTimeout(15000));
			log.info("Powerlink ad section found. keyword_id={}", keywordId);
			
			if (adSection.isVisible()) {
				// 광고 섹션 내의 모든 광고 목록을 추출
				// m: li.bx.img_type
				// p: li.lst.js-hover-item
				Locator adElements = adSection.locator(selectorAdElement);
				
				// 최소 하나의 광고가 나타날 때까지 최대 10초 대기
				adElements.first().waitFor(new Locator.WaitForOptions().setTimeout(10000));
				int adsCount = adElements.count();
				log.info("Found {} powerlink ad elements. keyword_id={}", adsCount, keywordId);
				
				LinkedHashMap<String, Integer> adRanks = new LinkedHashMap<>();
				
				// 각 광고 아이템을 순회하며 목표 URL과 일치하는 광고를 찾음
				for (int inx = 0; inx < adsCount; inx++) {
					Locator currentAd = adElements.nth(inx);
					
					// 'lnk_url' 클래스를 가진 요소의 텍스트를 추출
					// 이 부분은 제공해주신 HTML 구조에 기반한 선택자입니다.
					// m: div.url_inner span.url
					// p: span.lnk_url_area a.lnk_url
					String displayUrlText = currentAd.locator(selectorDisplayUrl).textContent().trim();
					
					adRanks.put(displayUrlText, inx + 1);
					log.debug("Powerlink ad found. ad_display_url={} rank={} keyword_id={}", displayUrlText, inx + 1, keywordId);
				}
				
				// 최종 순위를 메시지 객체에 설정
				int clientRank = Optional.ofNullable(adRanks.get(message.getDisplayUrl()))
						.orElse(-1);
				
				message.setViewedRank(clientRank);
				message.setViewedSlot(adsCount);
				message.setCompetitorRanks(adRanks);
				
				log.info("Completed powerlink rank search successfully. keyword_id={} rank={} ads_count={}", keywordId, clientRank, adsCount);
			} else {
				log.warn("No powerlink ad section found. keyword_id={}", keywordId);
				message.setViewedRank(-1);
				message.setViewedSlot(0);
			}
		} catch (Exception e) {
			log.error("Failed to crawl powerlink ads. keyword_id={}", keywordId, e);
			message.setViewedRank(-1);
			message.setViewedSlot(0);
			throw e;
		}
	}
	
	private void getShoppingRank(BiddingResultMessage message) throws Exception {
		String keywordId = message.getKeywordId();
		String keyword = message.getKeyword();
		String displayUrl = message.getDisplayUrl();
		DeviceType deviceType = message.getDeviceType();
		
		log.info("Start shopping rank search. keyword_id={} keyword='{}' display_url={} device={}", keywordId, keyword, displayUrl, deviceType);
		
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
			log.debug("Navigated to shopping search results. keyword_id={} url={}", keywordId, searchUrl + keyword);
			
			// 광고 섹션 선택
			Locator adSection = page.locator(selectorAdSection);
			adSection.waitFor(new Locator.WaitForOptions().setTimeout(15000));
			log.info("Shopping ad section found. keyword_id={}", keywordId);
			
			if (adSection.isVisible()) {
				// 광고 아이템 목록
				Locator adElements = adSection.locator(selectorAdElement);
				
				adElements.first().waitFor(new Locator.WaitForOptions().setTimeout(10000));
				int adsCount = adElements.count();
				log.info("Found {} shopping ad elements. keyword_id={}", adsCount, keywordId);
				
				LinkedHashMap<String, Integer> adRanks = new LinkedHashMap<>();
				
				for (int inx = 0; inx < adsCount; inx++) {
					Locator currentAd = adElements.nth(inx);
					
					// URL 위치: 모바일 / PC 분기
					String displayUrlText = currentAd.locator(selectorDisplayUrl).textContent().trim();
					
					// 광고의 순위를 맵에 저장
					adRanks.put(displayUrlText, inx + 1);
					log.debug("Shopping ad found. ad_display_url={} rank={} keyword_id={}", displayUrlText, inx + 1, keywordId);
				}
				
				// 최종 순위를 메시지 객체에 설정
				int clientRank = Optional.ofNullable(adRanks.get(message.getDisplayUrl()))
						.orElse(-1);
				
				message.setViewedRank(clientRank);
				message.setViewedSlot(adsCount);
				message.setCompetitorRanks(adRanks);
				
				log.info("Completed shopping rank search successfully. keyword_id={} rank={} ads_count={}", keywordId, clientRank, adsCount);
			} else {
				log.warn("No shopping ad section found. keyword_id={}", keywordId);
				message.setViewedRank(-1);
				message.setViewedSlot(0);
			}
		} catch (Exception e) {
			log.error("Failed to crawl shopping ads. keyword_id={}", keywordId, e);
			message.setViewedRank(-1);
			message.setViewedSlot(0);
			throw e;
		}
	}
	
}

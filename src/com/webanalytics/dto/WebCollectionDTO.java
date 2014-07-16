package com.webanalytics.dto;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class WebCollectionDTO {
	
	public static final String PAGE_STREAM = "pageStream";
	public static final String CLICK_STREAM = "clickStream";
	
	private String appId="";
	
	private Long timeCollected=0L;
	private String requestedFromIp="";
	private String page="";
	private String dataType="";
	private String clickDivInformation="";
	private String header="";
	private String userAgent="";
	private String referrer ="";
	
	public String getReferrer() {
		return referrer;
	}
	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}
	public String getHeader() {
		return header;
	}
	public void setHeader(String header) {
		this.header = header;
	}
	public String getUserAgent() {
		return userAgent;
	}
	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}
	public Long getTimeCollected() {
		return timeCollected;
	}
	public void setTimeCollected(Long timeCollected) {
		this.timeCollected = timeCollected;
	}
	public String getRequestedFromIp() {
		return requestedFromIp;
	}
	public void setRequestedFromIp(String requestedFromIp) {
		this.requestedFromIp = requestedFromIp;
	}
	public String getPage() {
		return page;
	}
	public void setPage(String page) {
		this.page = page;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public String getClickDivInformation() {
		return clickDivInformation;
	}
	public void setClickDivInformation(String clickDivInformation) {
		this.clickDivInformation = clickDivInformation;
	}
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	
}

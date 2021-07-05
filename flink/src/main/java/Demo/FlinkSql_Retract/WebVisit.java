package Demo.FlinkSql_Retract;

import java.util.Date;

public class WebVisit {

    public WebVisit() {
    }

    public WebVisit(String ip, String cookieId, String pageUrl, Date openTime, long openTimestamp, String browser) {
        this.ip = ip;
        this.cookieId = cookieId;
        this.pageUrl = pageUrl;
        this.openTime = openTime;
        this.openTimestamp = openTimestamp;
        this.browser = browser;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getCookieId() {
        return cookieId;
    }

    public void setCookieId(String cookieId) {
        this.cookieId = cookieId;
    }

    public String getPageUrl() {
        return pageUrl;
    }

    public void setPageUrl(String pageUrl) {
        this.pageUrl = pageUrl;
    }

    public Date getOpenTime() {
        return openTime;
    }

    public void setOpenTime(Date openTime) {
        this.openTime = openTime;
    }

    public long getOpenTimestamp() {
        return openTimestamp;
    }

    public void setOpenTimestamp(long openTimestamp) {
        this.openTimestamp = openTimestamp;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    @Override
    public String toString() {
        return "WebVisit{" +
                "ip='" + ip + '\'' +
                ", cookieId='" + cookieId + '\'' +
                ", pageUrl='" + pageUrl + '\'' +
                ", openTime=" + openTime +
                ", openTimestamp=" + openTimestamp +
                ", browser='" + browser + '\'' +
                '}';
    }

    private String ip;
    private String cookieId;
    private String pageUrl;
    private Date openTime;
    private long openTimestamp;
    private String browser;


}

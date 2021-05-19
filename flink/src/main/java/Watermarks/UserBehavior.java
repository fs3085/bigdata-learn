package Watermarks;

public class UserBehavior {
    private String userID;
    private Long eventTime;
    private String eventType;
    private Integer productID;

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userID='" + userID + '\'' +
                ", eventTime=" + eventTime +
                ", eventType='" + eventType + '\'' +
                ", productID=" + productID +
                '}';
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Integer getProductID() {
        return productID;
    }

    public void setProductID(Integer productID) {
        this.productID = productID;
    }

    public UserBehavior(String userID, Long eventTime, String eventType, Integer productID) {
        this.userID = userID;
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.productID = productID;
    }
}

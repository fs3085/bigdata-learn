package SqlApI.Temporal;

public class UserBrowseLog {
    public UserBrowseLog() {
    }

    public UserBrowseLog(String userID, String eventTime, long eventTimeTimestamp, String eventType, String productID, int productPrice) {
        this.userID = userID;
        this.eventTime = eventTime;
        this.eventTimeTimestamp = eventTimeTimestamp;
        this.eventType = eventType;
        this.productID = productID;
        this.productPrice = productPrice;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public long getEventTimeTimestamp() {
        return eventTimeTimestamp;
    }

    public void setEventTimeTimestamp(long eventTimeTimestamp) {
        this.eventTimeTimestamp = eventTimeTimestamp;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getProductID() {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public int getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(int productPrice) {
        this.productPrice = productPrice;
    }

    private String userID;
    private String eventTime;
    private long eventTimeTimestamp;
    private String eventType;
    private String productID;
    private int productPrice;



}

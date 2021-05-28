package SqlApI.Temporal;

public class ProductInfo {
    public ProductInfo(String productID, String productName, String productCategory, String updatedAt, long updatedAtTimestamp) {
        this.productID = productID;
        this.productName = productName;
        this.productCategory = productCategory;
        this.updatedAt = updatedAt;
        this.updatedAtTimestamp = updatedAtTimestamp;
    }

    public ProductInfo() {
    }

    public String getProductID() {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductCategory() {
        return productCategory;
    }

    public void setProductCategory(String productCategory) {
        this.productCategory = productCategory;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public long getUpdatedAtTimestamp() {
        return updatedAtTimestamp;
    }

    public void setUpdatedAtTimestamp(long updatedAtTimestamp) {
        this.updatedAtTimestamp = updatedAtTimestamp;
    }

    private String productID;
    private String productName;
    private String productCategory;
    private String updatedAt;
    private long updatedAtTimestamp;
}

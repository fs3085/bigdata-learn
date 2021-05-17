package flink_to_ck;

import java.io.Serializable;

public class Ck implements Serializable {
    public Ck() {
    }

    @Override
    public String toString() {
        return "{" +
                "userid='" + userid + '\'' +
                ", items='" + items + '\'' +
                ", create_date='" + create_date + '\'' +
                '}';
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getItems() {
        return items;
    }

    public void setItems(String items) {
        this.items = items;
    }

    public String getCreate_date() {
        return create_date;
    }

    public void setCreate_date(String create_date) {
        this.create_date = create_date;
    }

    public Ck(String userid, String items, String create_date) {
        this.userid = userid;
        this.items = items;
        this.create_date = create_date;
    }

    private String userid;
    private String items;
    private String create_date;
}

package cn._51doit.day10;

public class OrderBean {

    public String oid;

    public String cid;

    public Double money;

    public Double longitude;

    public Double latitude;

    public String province;

    public String city;

    public OrderBean(){}

    public OrderBean(String oid, String cid, Double money, Double longitude, Double latitude) {
        this.oid = oid;
        this.cid = cid;
        this.money = money;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public static OrderBean of(String oid, String cid, Double money, Double longitude, Double latitude) {
        return new OrderBean(oid, cid, money, longitude, latitude);
    }

    @Override
    public String toString() {
        return "OrderBean{" + "oid='" + oid + '\'' + ", cid='" + cid + '\'' + ", money=" + money + ", longitude=" + longitude + ", latitude=" + latitude + ", province='" + province + '\'' + ", city='" + city + '\'' + '}';
    }
}

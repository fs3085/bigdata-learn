package Split_SelectAndSide_Outputs;

public class PersonInfo {
    String name;
    String province;
    String city;
    int age;
    String idCard;
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getProvince() {
        return province;
    }
    public void setProvince(String province) {
        this.province = province;
    }
    public String getCity() {
        return city;
    }
    public void setCity(String city) {
        this.city = city;
    }
    public int getAge() {
        return age;
    }
    public void setAge(int age) {
        this.age = age;
    }
    public String getIdCard() {
        return idCard;
    }
    public void setIdCard(String idCard) {
        this.idCard = idCard;
    }
    public String toString(){
        return "name:"+name +" province:"+province+" city:"+city+" age:"+age+" idCard"+idCard;
    }
}

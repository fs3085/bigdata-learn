package Demo.Flink_Async.Mysql;

public class Store {
    private int Id;
    private String Name;

    public Store() {
    }

    public int getId() {
        return Id;
    }

    public void setId(int id) {
        Id = id;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public Store(int id, String name) {
        Id = id;
        Name = name;
    }
}

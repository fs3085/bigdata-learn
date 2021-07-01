package Demo.Flink_Async.Mysql;

public class Click {
    private int Id;
    private String Name;

    public Click() {
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

    public Click(int id, String name) {
        Id = id;
        Name = name;
    }
}

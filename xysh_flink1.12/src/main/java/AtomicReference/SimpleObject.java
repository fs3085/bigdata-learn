package AtomicReference;

public class SimpleObject {
    private String name;
    private Integer age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    @Override
    public String toString() {
        return "SimpleObject{" +
            "name='" + name + '\'' +
            ", age=" + age +
            '}';
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public SimpleObject(String name, Integer age) {
        this.name = name;
        this.age = age;
    }
}

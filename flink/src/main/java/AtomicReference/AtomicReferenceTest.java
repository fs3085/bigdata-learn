package AtomicReference;

import java.util.concurrent.atomic.AtomicReference;


public class AtomicReferenceTest {
    public static void main(String[] args) {
        //1、使用 null 初始值创建新的 AtomicReference。
        AtomicReference<SimpleObject> atomicReference = new AtomicReference<>();
        atomicReference.set(new SimpleObject("test1", 10));
        SimpleObject simpleObject = atomicReference.get();
        System.out.println("simpleObject  Value: " + simpleObject.toString());



        //2、使用给定的初始值创建新的 AtomicReference。
        AtomicReference<SimpleObject> atomicReference1 = new AtomicReference<>(new SimpleObject("test2",20));
        SimpleObject simpleObject1 = atomicReference1.get();
        System.out.println("simpleObject  Value: " + simpleObject1.toString());

        //3、如果当前值 == 预期值，则以原子方式将该值设置为给定的更新值。
        SimpleObject test = new SimpleObject("test3" , 30);
        AtomicReference<SimpleObject> atomicReference2 = new AtomicReference<>(test);
        Boolean bool = atomicReference2.compareAndSet(test, new SimpleObject("test4", 40));
        System.out.println("simpleObject  Value: " + bool);

        //4、以原子方式设置为给定值，并返回旧值，先获取当前对象，在设置新的对象
        SimpleObject test1 = new SimpleObject("test5" , 50);
        AtomicReference<SimpleObject> atomicReference3 = new AtomicReference<>(test1);
        SimpleObject simpleObject2 = atomicReference3.getAndSet(new SimpleObject("test6",50));
        SimpleObject simpleObject3 = atomicReference3.get();
        System.out.println("simpleObject  Value: " + simpleObject2.toString());
        System.out.println("simpleObject  Value: " + simpleObject3.toString());
    }
}

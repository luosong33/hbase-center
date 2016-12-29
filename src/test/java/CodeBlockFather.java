public class CodeBlockFather {
    static {
        System.out.println("父类静态代码块");
    }

    public CodeBlockFather() {
        System.out.println("父类构造函数");
    }

    {
        System.out.println("父类普通代码块");
    }
}
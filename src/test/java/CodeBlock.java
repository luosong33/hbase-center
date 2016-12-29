public class CodeBlock extends CodeBlockFather {
    static {
        System.out.println("子类静态代码块");
    }

    public CodeBlock() {
        System.out.println("子类构造函数");
    }

    {
        System.out.println("子类普通代码块");
    }

    public static void main(String[] args) {
        CodeBlock codeBlock = new CodeBlock();
    }
}
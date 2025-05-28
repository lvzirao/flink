/**
 * @Package PACKAGE_NAME.Reverse
 * @Author lv.zirao
 * @Date 2025/5/28 16:01
 * @description:
 */
public class Reverse {
    public static String reverseString(String s) {
        return new StringBuilder(s).reverse().toString();
    }
    public static void main(String[] args) {
        System.out.println(reverseString("Hello World"));
    }
}

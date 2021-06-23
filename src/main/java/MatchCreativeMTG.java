import java.util.regex.Pattern;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/6/22 13:44
 **/

public class MatchCreativeMTG {

    public static void main(String[] args) {
        String str = "pl-4792-dfnh_fb_ 0415_tw_1080x1080_v_ly_4791 (23844532747420677)";

//        System.out.println(str.split("\\(")[1].split("\\)")[0]);

        int sum = 0;
        for (int i=1; i<=364; i++) {
            sum += i;
        }
        // 795-430 = 365

//        System.out.println(sum);
        test(1,2,3);
    }


    static void test(int... s) {
        for (int i = 0; i < s.length; i++) {

            System.out.println(s[i]);
        }
    }

}

package com.alibaba.dt.udf;

import com.aliyun.odps.udf.UDF;

public class LexiLabelCompare extends UDF {
    public boolean lexicalLarger(String A, String B){
        A = "".equals(A) ? "0": A;
        B = "".equals(B) ? "0": B;
        String[] AList = A.split(",");
        String[] BList = B.split(",");
        if(AList.length > BList.length ){
            for(int i = 0; i < AList.length; i++){
                long x  = Long.parseLong(AList[i]);
                long y = 0;
                if(i < BList.length){
                    if("null".equals(BList[i])){
                        y = Long.MAX_VALUE;
                    }else {
                        y = Long.parseLong(BList[i]);
                    }
                }
                if(x < y){
                    return true;
                } else if (x > y){
                    return false;
                }
            }
        }else {
            for(int i = 0; i < BList.length; i++){
                long y;

                if("null".equals(BList[i])){
                    y = Long.MAX_VALUE;
                }else {
                    y = Long.parseLong(BList[i]);
                }

                long x = 0;
                if(i < AList.length){
                    x = Long.parseLong(AList[i]);
                }
                if(x < y){
                    return true;
                } else if (x > y){
                    return false;
                }
            }
        }
        return false;
    }

    public boolean lexicalStrictLarger(String A, String B){

        if(A.length() >= B.length()){
            return false;
        }else {
            String[] AList = A.split(",");
            String[] BList = B.split(",");
            if(AList.length >= BList.length){
                return false;
            }else {
                for(int i = 0; i< AList.length; i++){
                    if(!AList[i].equals(BList[i])){
                        return false;
                    }
                }
                return true;
            }
        }
    }

    public String evaluate(String labelA, String labelB, String type) {
        try{
            if("strict".equals(type)){
                return lexicalStrictLarger(labelA, labelB)? "1":"0";
            }else {
                return lexicalLarger(labelA, labelB)? "1":"0";
            }
        }catch (Throwable e){
            e.printStackTrace();
            return "wrong";
        }
    }

    public static void main(String[] args) {
        LexiLabelCompare ins = new LexiLabelCompare();
        System.out.println(ins.evaluate("59138,2,13,2,47,122","59138,2,13,2,47,122,61", "strict"));
    }
}

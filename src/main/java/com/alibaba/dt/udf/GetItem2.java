package com.alibaba.dt.udf;

import com.aliyun.odps.udf.UDF;
import com.google.common.collect.Lists;
import org.ujmp.core.Matrix;
import org.ujmp.core.calculation.Calculation.Ret;

import java.math.BigDecimal;
import java.util.List;

public class GetItem2 extends UDF {
    Matrix matrixA;
    long p;
    long coe = 1;

    public static void main(String[] args) {
        String AMatrixNullZeroValues =
              "1::1::0.1249999999998,1::2::0.249999999996,"
            + "2::1::0.124999995,2::2::0.25";
        //4::4::0.8,4::5::0.2,5::4::0.2,5::5::0.8
        String BMatrixNullZeroValues =
              "1::1::0.2499999999998,1::2::0.5,"
            + "2::1::0.5";
        GetItem2 ins = new GetItem2();
        System.out.println(ins.evaluate(Lists.newArrayList(AMatrixNullZeroValues.split(",")), "2","2"));
    }

    public List<String> evaluate(List<String> matrixA, String rowNumA, String columnNumA) {
        if(!initialMatrixSize(rowNumA, columnNumA)){
            return Lists.newArrayList();
        }

        setMatrixAAndBAndSrcAndLeaf(matrixA);

        Matrix item2 = getItem2();

        System.out.println(item2);

        List<String> sj = Lists.newArrayList();

        for(int i = 0; i< item2.getRowCount(); i++){
            for(int j = 0; j < item2.getColumnCount(); j++) {
                double v = item2.getAsBigDecimal(i, j).doubleValue();
                if (v > 0 || v < 0) {
                    sj.add( (i+1) + "::" +(j+1) + "::" + v);
                }
            }
        }
        return sj;
    }

    public boolean initialMatrixSize(String rowNumA, String columnNumA){
        long x = Long.parseLong(rowNumA);
        long y = Long.parseLong(columnNumA);
        if(x == y){
            p = x;
        }else {
            return false;
        }
        return true;
    }

    public void setMatrixAAndBAndSrcAndLeaf(List<String> A){
        this.matrixA = stringArrayToMatrix(A, p , p, true);
    }

    public Matrix getItem2(){
        Matrix ornamentalA = getOrnamentalA();

        Matrix I2p = Matrix.Factory.eye(2*p,2*p).times(coe);

        Matrix fracDown = I2p.minus(ornamentalA.times(coe));

        Matrix frac2 = fracDown.inv();

        return I2p.mtimes(frac2);
    }

    public void printMatrix(Matrix matrix){
        long rowCnt = matrix.getRowCount();
        long columnCnt = matrix.getColumnCount();
        for(long i = 0; i< rowCnt; i++){
            for(long j = 0; j< columnCnt; j++){
                System.out.print(matrix.getAsBigDecimal(i, j));
                if(j<columnCnt-1){
                    System.out.print(",");
                }
            }
            System.out.print("\n");
        }
    }

    public void setCoe(double max, double min){
        double reciprocalMin = 1/min;
        double reciprocalMax = 1/max;
        if(min > 0.01){
            return;
        }
        else {
            int p = ((int) Math.log10(reciprocalMin) + (int) Math.log10(reciprocalMax)) /2;
            coe  = (long) Math.pow(10, p);
        }
    }

    public Matrix getOrnamentalA(){
        Matrix LU = matrixA.times(0.5);
        Matrix RU = Matrix.Factory.eye(p,p);
        Matrix LD = Matrix.Factory.eye(p,p).times(0.5);
        Matrix RD = Matrix.Factory.zeros(p,p);
        return combineFourMatrices(LU, RU, LD, RD);
    }

    public Matrix combineFourMatrices(Matrix A, Matrix B, Matrix C, Matrix D){
        Matrix AB  = A.appendHorizontally(Ret.NEW, B);
        Matrix CD  = C.appendHorizontally(Ret.NEW, D);
        return AB.appendVertically(Ret.NEW, CD);
    }
    
    public Matrix stringArrayToMatrix(List<String> oneLineArray, long rn, long cn, boolean ifSetMaxMin) {
        Matrix matrix = Matrix.Factory.zeros(rn, cn);
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        for(String rowValue : oneLineArray){
            String[] rowTrip = rowValue.split("::");
            long i = Long.parseLong(rowTrip[0].trim())-1;
            long j = Long.parseLong(rowTrip[1].trim())-1;
            double value = Double.parseDouble(rowTrip[2].trim());
            BigDecimal v= BigDecimal.valueOf(value);
            if(ifSetMaxMin){
                min = Math.min(min, value);
                max = Math.max(max, value);
            }
            matrix.setAsBigDecimal(v, i, j);
        }
        if(ifSetMaxMin) {
            setCoe(max, min);
        }
        return matrix;
    }
    
}

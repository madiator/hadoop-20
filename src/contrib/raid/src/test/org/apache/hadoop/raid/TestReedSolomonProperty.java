package org.apache.hadoop.raid;

import java.util.Random;

import junit.framework.TestCase;

public class TestReedSolomonProperty extends TestCase {
  final Random RAND = new Random();
  private final GaloisField GF = GaloisField.getInstance(256, 285);
  
  public void testRSProperty() {
    int stripeSize = 50;    
    int paritySizeRS = 4;
    int simpleParityDegree = 6;
    int paritySizeSRC = 0;
    if (simpleParityDegree == 0)
      paritySizeSRC = 0;
    else 
      paritySizeSRC = (stripeSize + paritySizeRS)/simpleParityDegree;
    
    int paritySize = paritySizeRS + paritySizeSRC; 
    ErasureCode ec = new ReedSolomonCode(stripeSize, paritySize, simpleParityDegree);
    //for (int m = 0; m < TEST_TIMES; m++) {
      int symbolMax = (int) Math.pow(2, ec.symbolSize());
      int[] message = new int[stripeSize];
      for (int i = 0; i < stripeSize; i++) {
        message[i] = RAND.nextInt(symbolMax);
      }
      int[] parity = new int[paritySize];
      ec.encode(message, parity);
      int[] data = new int[stripeSize + paritySize];
      int[] copy = new int[data.length];
      for (int i = 0; i < paritySize; i++) {
        data[i] = parity[i];
        copy[i] = parity[i];
      }
      for (int i = 0; i < stripeSize; i++) {
        data[i + paritySize] = message[i];
        copy[i + paritySize] = message[i];
      }
      
      for(int i = 0; i < paritySizeSRC; i++)
        System.out.print(data[i]+",");
      System.out.println();
      int p = 0;
      int full = 0;
      for (int i = 0; i < paritySizeSRC; i++) {
        p = 0;
        for (int f = 0; f < simpleParityDegree; f++) {
          p = GF.add(data[paritySizeSRC+i*simpleParityDegree+f], p);
        }
        full = GF.add(full, p);
        System.out.print(full+",");
      }
      System.out.println(full);
  }

}

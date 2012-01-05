package org.apache.hadoop.raid;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

public class TestUSCEncodeDecode extends TestCase {
  final int TEST_TIMES = 1000;
  final Random RAND = new Random();
  private int[] randomErasedLocation(int erasedLen, int dataLen) {
    int[] erasedLocations = new int[erasedLen];
    for (int i = 0; i < erasedLen; i++) {
      Set<Integer> s = new HashSet<Integer>();
      while (s.size() != erasedLen) {
        s.add(RAND.nextInt(dataLen));
      }
      int t = 0;
      for (int erased : s) {
        erasedLocations[t++] = erased;
      }
    }
    return erasedLocations;
  }

  public void testEncodeDecode() {
    int stripeSize = 10;
    int paritySizeRS = 4;
    int paritySizeSRC = 2;
    int paritySize = paritySizeRS + paritySizeSRC;
    ErasureCode ec = new ReedSolomonCode(stripeSize, paritySizeRS, paritySizeSRC);
    for (int m = 0; m < TEST_TIMES; m++) {
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
      //int erasedLen = paritySizeRS == 1 ? 1 : RAND.nextInt(paritySizeRS - 1) + 1;
      int erasedLen = 4; //paritySizeRS;
      int[] erasedLocations = randomErasedLocation(erasedLen, data.length);
      //int[] erasedLocations = new int[]{1, 3};
      for (int i = 0; i < erasedLocations.length; i++) {
        data[erasedLocations[i]] = 0;
      }
      int[] erasedValues = new int[erasedLen];
      ec.decode(data, erasedLocations, erasedValues);
      for (int i = 0; i < erasedLen; i++) {
        //assertEquals("Decode failed", copy[erasedLocations[i]], erasedValues[i]);
        if(copy[erasedLocations[i]]!=erasedValues[i]) {
          System.out.println("(stripeSize, paritySizeSRC, paritySizeRS) = ("+stripeSize+", "+paritySizeSRC+", "+paritySizeRS+")");
          for(int fld = 0; fld < erasedLocations.length; fld++)
            System.out.println(erasedLocations[fld]);

          break;
        }
      }
    }
  }

  public void testRSPerformance() {
    int stripeSize = 10;
    int paritySizeRS = 4;
    int paritySizeSRC = 2;
    int paritySize = paritySizeRS + paritySizeSRC;
    ErasureCode ec = new ReedSolomonCode(stripeSize, paritySizeRS, paritySizeSRC);
    int symbolMax = (int) Math.pow(2, ec.symbolSize());
    byte[][] message = new byte[stripeSize][];
    int bufsize = 10*1024*1024;
    for (int i = 0; i < stripeSize; i++) {
      message[i] = new byte[bufsize];
      for (int j = 0; j < bufsize; j++) {
        message[i][j] = (byte) RAND.nextInt(symbolMax);
      }
    }
    byte[][] parity = new byte[paritySize][];
    for (int i = 0; i < paritySize; i++) {
      parity[i] = new byte[bufsize];
    }
    long encodeStart = System.currentTimeMillis();
    int[] tmpIn = new int[stripeSize];
    int[] tmpOut = new int[paritySize];
    for (int i = 0; i < bufsize; i++) {
      // Copy message.
      for (int j = 0; j < stripeSize; j++) tmpIn[j] = 0x000000FF & message[j][i];
      ec.encode(tmpIn, tmpOut);
      // Copy parity.
      for (int j = 0; j < paritySize; j++) parity[j][i] = (byte)tmpOut[j];
    }
    long encodeEnd = System.currentTimeMillis();
    float encodeMSecs = (encodeEnd - encodeStart);
    System.out.println("Time to encode rs = " + encodeMSecs +
      "msec (" + message[0].length / (1000 * encodeMSecs) + " MB/s)");

    // Copy erased array.
    int[] data = new int[paritySize + stripeSize];
    // The 0th symbol in the message is at paritySize
    // make sure the below indices work.
    int[] erasedLocations = new int[]{paritySize, 0};//, 10};//, 5, 7};
    int[] erasedValues = new int[erasedLocations.length];
    byte[] copy = new byte[bufsize];
    for (int j = 0; j < bufsize; j++) {
      copy[j] = message[0][j];
      message[0][j] = 0;
    }

    for(int tests = 0; tests < 5; tests++) {
    long decodeStart = System.currentTimeMillis();
    for (int i = 0; i < bufsize; i++) {
      // Copy parity first.
      for (int j = 0; j < paritySize; j++) {
        data[j] = 0x000000FF & parity[j][i];
      }
      // Copy message. Skip 0 as the erased symbol
      for (int j = 1; j < stripeSize; j++) {
        data[j + paritySize] = 0x000000FF & message[j][i];
      }
      ec.decode(data, erasedLocations, erasedValues);
      message[0][i] = (byte)erasedValues[0];
    }
    long decodeEnd = System.currentTimeMillis();
    float decodeMSecs = (decodeEnd - decodeStart);
    System.out.println("Time to decode = " + decodeMSecs +
      "msec (" + message[0].length / (1000 * decodeMSecs) + " MB/s)");
    assertTrue("Decode failed", java.util.Arrays.equals(copy, message[0]));
    }
  }


}

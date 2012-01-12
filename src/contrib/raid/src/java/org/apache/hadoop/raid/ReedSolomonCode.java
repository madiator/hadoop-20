/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.raid;

import java.util.Set;

public class ReedSolomonCode implements ErasureCode {

  private final int stripeSize;
  private final int paritySize;
  private final int paritySizeRS;
  private final int paritySizeSRC;
  private final int paritySizeSRCFull;
  private final int simpleParityDegree;
  private final int[] generatingPolynomial;
  private final int PRIMITIVE_ROOT = 2;
  private final int[] primitivePower;
  private final GaloisField GF = GaloisField.getInstance(256, 285);
  private final int[] errSignature;
  private final int[] paritySymbolLocations;
  private final int[] dataBuff;
  private final int[][] locationsToUse;

  public ReedSolomonCode(int stripeSize, int paritySizeRS, int paritySizeSRC) {
    this.paritySizeRS = paritySizeRS;
    this.paritySizeSRC = paritySizeSRC;
    this.paritySize = paritySizeRS + paritySizeSRC;
    this.stripeSize = stripeSize;
    assert(stripeSize + paritySize < GF.getFieldSize());

    if(paritySizeSRC>0) {
      paritySizeSRCFull = paritySizeSRC + 1;
      simpleParityDegree = (int)Math.ceil((float)(stripeSize + paritySizeRS)/(float)paritySizeSRCFull);
    }
    else {
      simpleParityDegree = -1;
      this.paritySizeSRCFull = 0;
    }

    this.errSignature = new int[paritySizeRS];
    this.paritySymbolLocations = new int[paritySizeRS + paritySizeSRCFull];
    this.dataBuff = new int[paritySizeRS + stripeSize];

    for (int i = 0; i < paritySizeRS + paritySizeSRCFull; i++) {
      paritySymbolLocations[i] = i;
    }

    this.primitivePower = new int[stripeSize + paritySizeRS];
    // compute powers of the primitive root
    for (int i = 0; i < stripeSize + paritySizeRS; i++) {
      primitivePower[i] = GF.power(PRIMITIVE_ROOT, i);
    }
    // compute generating polynomial
    int[] gen  = {1};
    int[] poly = new int[2];
    for (int i = 0; i < paritySizeRS; i++) {
      poly[0] = primitivePower[i];
      poly[1] = 1;
      gen 	= GF.multiply(gen, poly);
    }
    // generating polynomial has all generating roots
    generatingPolynomial = gen;

    locationsToUse = new int[][]{
        {6, 7, 8, 9, 10},
        {11, 12, 13, 14, 15},

        {0, 1, 3, 4, 5},
        {0, 1, 2, 4, 5},
        {0, 1, 2, 3, 5},
        {0, 1, 2, 3, 4},

        {0, 7, 8, 9, 10},
        {0, 6, 8, 9, 10},
        {0, 6, 7, 9, 10},
        {0, 6, 7, 8, 10},
        {0, 6, 7, 8, 9},

        {1, 12, 13, 14, 15},
        {1, 11, 13, 14, 15},
        {1, 11, 12, 14, 15},
        {1, 11, 12, 13, 15},
        {1, 11, 12, 13, 14},
        };


  }

  @Override
  public void encode(int[] message, int[] parity) {
    assert(message.length == stripeSize && parity.length == paritySize);

    for (int i = 0; i < paritySizeRS; i++) {
      dataBuff[i] = 0;
    }
    for (int i = 0; i < stripeSize; i++) {
      dataBuff[i + paritySizeRS] = message[i];
    }
    GF.remainder(dataBuff, generatingPolynomial);
    for (int i = 0; i < paritySizeRS; i++) {
      parity[paritySizeSRC + i] = dataBuff[i];
    }
    for (int i = 0; i < stripeSize; i++) {
      dataBuff[i + paritySizeRS] = message[i];
    }
    for(int i = 0; i < paritySizeSRC; i++) {
      parity[i] = 0;
      for(int j = 0; j < simpleParityDegree; j++)
        parity[i] = GF.add(dataBuff[simpleParityDegree*(i + 1) + j - 1], parity[i]);
    }
  }


  @Override
  public void decode(int[] data, int[] erasedLocation, int[] erasedValue) {
    if (erasedLocation.length == 0) {
      return;
    }
    assert(erasedLocation.length == erasedValue.length);
    //boolean isErased[] = new boolean[data.length];
    for (int i = 0; i < erasedLocation.length; i++) {
      data[erasedLocation[i]] = 0;
      //isErased[erasedLocation[i]] = true;
    }

    int sum = 0;

    if(erasedLocation.length == 1) {
      sum = 0;
      for(int i = 0; i < 5; i++) {
        sum = GF.add(sum, data[locationsToUse[erasedLocation[0]][i]]);
      }
      erasedValue[0] = sum;
    }
    else {
      // otherwise do RS decoding.
      int[] dataRS = new int[paritySizeRS + stripeSize];
      int[] erasedValueRS = new int[paritySizeRS];
      int erasedLocationLengthRS = 0;
      for (int i = 0; i < erasedLocation.length; i++) {
        if (erasedLocation[i] >= paritySizeSRC)
          erasedLocationLengthRS++;
      }
      //Create a copy of the RS data
      for (int i = paritySizeSRC; i < data.length; i++) {
        dataRS[i - paritySizeSRC] = data[i];
      }

      int count = 0;
      for (int i = 0; i < erasedLocation.length; i++) {
        // if it is an RS block erasure
        if (erasedLocation[i] >= paritySizeSRC) {
          errSignature[count] =
            primitivePower[erasedLocation[i] - paritySizeSRC];
          erasedValueRS[count] =
            GF.substitute(dataRS, primitivePower[count]);
          count++;
        }
      }
      GF.solveVandermondeSystem(
              errSignature, erasedValueRS, erasedLocationLengthRS);
      count = 0;
      for (int j = 0; j < erasedLocation.length; j++) {
        if(erasedLocation[j] >= paritySizeSRC) {
          dataRS[erasedLocation[j] - paritySizeSRC] = erasedValueRS[count];
          erasedValue[j] = erasedValueRS[count];
          data[erasedLocation[j]] = erasedValueRS[count];
          count++;
        }
      }
      // then check if there are any simpleXOR parities erased
      for (int i = 0; i < erasedLocation.length; i++) {
        if (erasedLocation[i] < paritySizeSRC) {
          sum = 0;
          for(int j = 0; j < 5; j++) {
            sum = GF.add(sum, data[locationsToUse[erasedLocation[i]][j]]);
          }
          erasedValue[i] = sum;
        }
      }
    }
  }

  private void doRSDecode(int[] data, int[] erasedLocation, int[] erasedValue) {
    for (int i = 0; i < erasedLocation.length; i++) {
      errSignature[i] = primitivePower[erasedLocation[i]];
      erasedValue[i] = GF.substitute(data, primitivePower[i]);
    }
    GF.solveVandermondeSystem(errSignature, erasedValue, erasedLocation.length);
  }

  @Override
  public int stripeSize() {
    return this.stripeSize;
  }

  @Override
  public int paritySize() {
    return this.paritySize;
  }

  @Override
  public int symbolSize() {
    return (int) Math.round(Math.log(GF.getFieldSize()) / Math.log(2));
  }

  /**
   * Given parity symbols followed by message symbols, return the locations of
   * symbols that are corrupted. Can resolve up to (parity length / 2) error
   * locations.
   * @param data The message and parity. The parity should be placed in the
   *             first part of the array. In each integer, the relevant portion
   *             is present in the least significant bits of each int.
   *             The number of elements in data is stripeSize() + paritySize().
   *             <b>Note that data may be changed after calling this method.</b>
   * @param errorLocations The set to put the error location results
   * @return true If the locations can be resolved, return true.
   */
  public boolean computeErrorLocations(int[] data,
      Set<Integer> errorLocations) {
    assert(data.length == paritySize + stripeSize && errorLocations != null);
    errorLocations.clear();
    int maxError = paritySizeRS / 2;
    int[][] syndromeMatrix = new int[maxError][];
    for (int i = 0; i < syndromeMatrix.length; ++i) {
      syndromeMatrix[i] = new int[maxError + 1];
    }
    int[] syndrome = new int[paritySizeRS];

    if (computeSyndrome(data, syndrome)) {
      // Parity check OK. No error location added.
      return true;
    }
    for (int i = 0; i < maxError; ++i) {
      for (int j = 0; j < maxError + 1; ++j) {
        syndromeMatrix[i][j] = syndrome[i + j];
      }
    }
    GF.gaussianElimination(syndromeMatrix);
    int[] polynomial = new int[maxError + 1];
    polynomial[0] = 1;
    for (int i = 0; i < maxError; ++i) {
      polynomial[i + 1] = syndromeMatrix[maxError - 1 - i][maxError];
    }
    for (int i = 0; i < paritySize + stripeSize; ++i) {
      int possibleRoot = GF.divide(1, primitivePower[i]);
      if (GF.substitute(polynomial, possibleRoot) == 0) {
        errorLocations.add(i);
      }
    }
    // Now recover with error locations and check the syndrome again
    int[] locations = new int[errorLocations.size()];
    int k = 0;
    for (int loc : errorLocations) {
      locations[k++] = loc;
    }
    int [] erasedValue = new int[locations.length];
    decode(data, locations, erasedValue);
    for (int i = 0; i < locations.length; ++i) {
      data[locations[i]] = erasedValue[i];
    }
    return computeSyndrome(data, syndrome);
  }

  /**
   * Compute the syndrome of the input [parity, message]
   * @param data [parity, message]
   * @param syndrome The syndromes (checksums) of the data
   * @return true If syndromes are all zeros
   */
  private boolean computeSyndrome(int[] data, int [] syndrome) {
    boolean corruptionFound = false;
    for (int i = 0; i < paritySizeRS; i++) {
      syndrome[i] = GF.substitute(data, primitivePower[i]);
      if (syndrome[i] != 0) {
        corruptionFound = true;
      }
    }
    return !corruptionFound;
  }

  public int[][] getLocationsToUse() {
    return locationsToUse;
  }
}

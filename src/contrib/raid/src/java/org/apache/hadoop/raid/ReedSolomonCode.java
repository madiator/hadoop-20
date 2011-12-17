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
  private final int simpleParityDegree;
  private final int[] generatingPolynomial;
  private final int PRIMITIVE_ROOT = 2;
  private final int[] primitivePower;
  private final GaloisField GF = GaloisField.getInstance(256, 285);
  private final int[] errSignature;
  private final int[] paritySymbolLocations;
  private final int[] dataBuff;

  public ReedSolomonCode(int stripeSize, int paritySizeRS, int paritySizeSRC) {
    this.paritySizeRS = paritySizeRS;
    this.paritySizeSRC = paritySizeSRC;
    this.paritySize = paritySizeRS + paritySizeSRC;
    this.stripeSize = stripeSize;
    assert(stripeSize + paritySize < GF.getFieldSize());

    if(paritySizeSRC>0)
      simpleParityDegree = (stripeSize + paritySizeRS)/paritySizeSRC;
    else
      simpleParityDegree = -1;

    //TODO: fix above else condition and make sure the code runs RS if paritySizeSRC = 0;


    this.errSignature = new int[paritySizeRS];
    this.paritySymbolLocations = new int[paritySizeRS+paritySizeSRC];
    this.dataBuff = new int[paritySizeRS + stripeSize];

    for (int i = 0; i < paritySizeRS+paritySizeSRC; i++) {
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
      parity[paritySizeSRC+i] = dataBuff[i];
    }
    for (int i = 0; i < stripeSize; i++) {
      dataBuff[i + paritySizeRS] = message[i];
    }
    for (int i = 0; i < paritySizeSRC; i++) {
      parity[i] = 0;
      for (int f = 0; f < simpleParityDegree; f++) {
        parity[i] = GF.add(dataBuff[i*simpleParityDegree+f], parity[i]);
      }
    }
  }

  @Override
  public void decode(int[] data, int[] erasedLocation, int[] erasedValue) {

    boolean[] isErased = new boolean[data.length];
    int erasureGroup = 0;
    int indexToUse = 0;
    if (erasedLocation.length == 0) {
        return;
    }
    assert(erasedLocation.length == erasedValue.length);

    for (int i = 0; i < data.length; i++) {
      isErased[i] = false;
    }
    //Make sure erased data and erased values are set to 0
    for (int i = 0; i < erasedLocation.length; i++) {
      data[erasedLocation[i]] = 0;
      erasedValue[i] = 0;
      isErased[erasedLocation[i]] = true;
    }

    // First see if you can fix the erasures using the simpleParities.
    // This can be done only when the number of erasures <= paritySizeSRC
    boolean failed = false;
    if(erasedLocation.length <= paritySizeSRC) {
      for (int i = 0; i < erasedLocation.length; i++) {
        // Finds its XOR Group
        if (erasedLocation[i] >= paritySizeSRC) {
          erasureGroup = (int) Math.ceil(
              ((float) (erasedLocation[i] - paritySizeSRC + 1))
              /
              ((float) simpleParityDegree)
              );
        }
        else
          erasureGroup = erasedLocation[i] + 1;

        // Now XOR them together. f = -1 to consider the simpleParity itself.
        for (int f = -1; f < simpleParityDegree; f++) {
          if(f==-1)
            indexToUse = erasureGroup - 1;
          else
            indexToUse = (erasureGroup - 1) * simpleParityDegree +
                          f + paritySizeSRC;
          if((indexToUse != erasedLocation[i]) && isErased[indexToUse]) {
            failed = true;
            break;
          }
          erasedValue[i] = GF.add(erasedValue[i], data[indexToUse]);
        }
        if(failed)
          break;
        isErased[erasedLocation[i]] = false; //this index has been recovered.
      }
    }
    else
      failed = true;
    // if the above didn't fail, then all erasures were fixed.
    if(!failed)
      return;

    // otherwise do RS decoding.
    int[] dataRS = new int[paritySizeRS + stripeSize];
    int[] erasedValueRS = new int[paritySizeRS];
    int erasedLocationLengthRS = 0;

    //Create a copy of the RS data
    for (int i = paritySizeSRC;
             i < stripeSize + paritySizeRS + paritySizeSRC; i++) {
      dataRS[i-paritySizeSRC] = data[i];
    }
    /*
     * reset all erasedValues to zero.
     * we could improve this by only reseting those values whose isErased values
     * are true.
     * Also count the number of RS erasures.
     */

    for (int i = 0; i < erasedLocation.length; i++) {
      erasedValue[i] = 0;
      //if it is an RS block erasure
      if (erasedLocation[i] >= paritySizeSRC)
        erasedLocationLengthRS++;
    }
    if (erasedLocationLengthRS >= 1) { //if there are RS failures
      int count = 0;
      for (int i = 0; i < erasedLocation.length; i++) {
        // if it is an RS block erasure
        if (erasedLocation[i] >= paritySizeSRC) {
          errSignature[count] =
            primitivePower[erasedLocation[i]-paritySizeSRC];
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
          dataRS[erasedLocation[j]-paritySizeSRC] = erasedValueRS[count];
          erasedValue[j] = erasedValueRS[count];
          count++;
        }
      }
    }
    // then check if there are any simpleXOR parities erased
    for (int i = 0; i < erasedLocation.length; i++) {
      if (erasedLocation[i] < paritySizeSRC) {
        for (int f = 0; f < simpleParityDegree; f++) {
            erasedValue[i]  = GF.add(erasedValue[i], dataRS[erasedLocation[i]*simpleParityDegree+f]);
        }
      }
    }
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
}

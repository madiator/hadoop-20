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

import com.sun.org.apache.commons.logging.LogFactory;

public class ReedSolomonCode extends ErasureCode {
  public static final Log LOG = LogFactory.getLog(ReedSolomonCode.class);

  private int stripeSize;
  private int paritySize;
  private int[] generatingPolynomial;
  private int PRIMITIVE_ROOT = 2;
  private int[] primitivePower;
  private GaloisField GF = GaloisField.getInstance();
  private int[] errSignature;
  private int[] paritySymbolLocations;
  private int[] dataBuff;

  @Deprecated
  public ReedSolomonCode(int stripeSize, int paritySize) {
    init(stripeSize, paritySize);
  }

  public ReedSolomonCode() {
  }

  @Override
  public void init(Codec codec) {
    init(codec.stripeLength, codec.parityLength);
    LOG.info("Initialized " + ReedSolomonCode.class +
        " stripeLength:" + codec.stripeLength +
        " parityLength:" + codec.parityLength);
  }

  private void init(int stripeSize, int paritySize) {
    assert(stripeSize + paritySize < GF.getFieldSize());
    this.stripeSize = stripeSize;
    this.paritySize = paritySize;
    this.errSignature = new int[paritySize];
    this.paritySymbolLocations = new int[paritySize];
    this.dataBuff = new int[paritySize + stripeSize];
    for (int i = 0; i < paritySize; i++) {
      paritySymbolLocations[i] = i;
    }

    this.primitivePower = new int[stripeSize + paritySize];
    // compute powers of the primitive root
    for (int i = 0; i < stripeSize + paritySize; i++) {
      primitivePower[i] = GF.power(PRIMITIVE_ROOT, i);
    }
    // compute generating polynomial
    int[] gen = {1};
    int[] poly = new int[2];
    for (int i = 0; i < paritySize; i++) {
      poly[0] = primitivePower[i];
      poly[1] = 1;
      gen = GF.multiply(gen, poly);
    }
    // generating polynomial has all generating roots
    generatingPolynomial = gen;
  }

  @Override
  public void encode(int[] message, int[] parity) {
    assert(message.length == stripeSize && parity.length == paritySize);
    for (int i = 0; i < paritySize; i++) {
      dataBuff[i] = 0;
    }
    for (int i = 0; i < stripeSize; i++) {
      dataBuff[i + paritySize] = message[i];
    }
    GF.remainder(dataBuff, generatingPolynomial);
    for (int i = 0; i < paritySize; i++) {
      parity[i] = dataBuff[i];
    }
  }


  @Override
  public void decode(int[] data, int[] erasedLocations, int[] erasedValues) {
    if (erasedLocations.length == 0) {
      return;
    }
    assert(erasedLocations.length == erasedValues.length);
    for (int i = 0; i < erasedLocations.length; i++) {
      data[erasedLocations[i]] = 0;
    }
    for (int i = 0; i < erasedLocations.length; i++) {
      errSignature[i] = primitivePower[erasedLocations[i]];
      erasedValues[i] = GF.substitute(data, primitivePower[i]);
    }
    GF.solveVandermondeSystem(errSignature, erasedValues,
        erasedLocations.length);
  }

  @Override
  public void decode(int[] data, int[] erasedLocation, int[] erasedValue,
      int[] locationsToRead, int[] locationsNotToRead) {

    /*
     * Pretend that all locations in locationsNotToRead are
     * erased and try to repair them.
     */
    int[] recovValue = new int[locationsNotToRead.length];

    decode(data, locationsNotToRead, recovValue);

    /*
     * Among the recovered values corresponding to locationsNotToRead
     * copy those corresponding to erasedLocation into erasedValue.
     */
    for (int i=0; i < erasedLocation.length; i++) {
      for (int j=0; j < locationsNotToRead.length; j++) {
        if (erasedLocation[i] == locationsNotToRead[j]) {
          erasedValue[i] = recovValue[j];
          break;
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
    int maxError = paritySize / 2;
    int[][] syndromeMatrix = new int[maxError][];
    for (int i = 0; i < syndromeMatrix.length; ++i) {
      syndromeMatrix[i] = new int[maxError + 1];
    }
    int[] syndrome = new int[paritySize];

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
    for (int i = 0; i < paritySize; i++) {
      syndrome[i] = GF.substitute(data, primitivePower[i]);
      if (syndrome[i] != 0) {
        corruptionFound = true;
      }
    }
    return !corruptionFound;
  }
}

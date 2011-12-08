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

	private final int 	stripeSize;
	private final int 	paritySize;
	private final int 	paritySizeRS;
	private final int 	paritySizeSRC;
	private final int 	simpleParityDegree;
	private final int[] generatingPolynomial;
	private final int 	PRIMITIVE_ROOT = 2;
	private final int[] primitivePower;
	private final GaloisField GF = GaloisField.getInstance(256, 285);
	private int[]		errSignature; 
	private final int[] paritySymbolLocations;
	private final int[] dataBuff;

  public ReedSolomonCode(int stripeSize, int paritySize, int simpleParityDegree) {
	  assert(stripeSize + paritySize < GF.getFieldSize());
		this.stripeSize = stripeSize;//k
		this.paritySize = paritySize;//n-k = n'/f+n'-k
		this.simpleParityDegree	= simpleParityDegree;
		this.paritySizeRS	= ((simpleParityDegree)*(paritySize+stripeSize))/(simpleParityDegree+1) - stripeSize ; // n' = (f+1)/f*n-k
		this.paritySizeSRC = paritySize-paritySizeRS;//n'/f;
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
		assert(message.length == stripeSize && parity.length == paritySizeRS+paritySizeSRC);

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
			for (int f = 0; f < simpleParityDegree; f++) {
				parity[i] = GF.add(dataBuff[i*simpleParityDegree+f], parity[i]);
			}
		}
	}

  @Override
  public void decode(int[] data, int[] erasedLocation, int[] erasedValue) {

    int[] dataRS = new int[paritySizeRS+stripeSize];
    int[] erasedValueRS = new int[paritySizeRS];
    int erasedLocationLengthRS = 0;        
    double singleErasureGroup = 0;

    if (erasedLocation.length == 0) {
        return;
    }
    assert(erasedLocation.length == erasedValue.length);

    //make sure erased data are set to 0
    for (int i = 0; i < erasedLocation.length; i++) {
        data[erasedLocation[i]] = 0;
    }
    // create a copy of the RS data
    for (int i = paritySizeSRC; i < stripeSize+paritySizeRS+paritySizeSRC; i++) {
        dataRS[i-paritySizeSRC] = data[i];            
    }
    //if more than 1 failure, do RS decode and reconstruct simpleXOR parities if needed
    if (erasedLocation.length > 1){
        //find the number of RS failures
        for (int i = 0; i < erasedLocation.length; i++) {
          if (erasedLocation[i]>=paritySizeSRC) { //if it is an RS block erasure
            erasedLocationLengthRS++;
          }
        }
          
        if (erasedLocationLengthRS >= 1) { //if there are RS failures
          int count = 0;            
            for (int i = 0; i < erasedLocation.length; i++) {
              if (erasedLocation[i]>=paritySizeSRC){// if it is an RS block erasure 
                errSignature[count] = primitivePower[erasedLocation[i]-paritySizeSRC];
                erasedValueRS[count] = GF.substitute(dataRS, primitivePower[count]);
                count++;                
              }
            }
            GF.solveVandermondeSystem(errSignature, erasedValueRS, erasedLocationLengthRS);
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
            if (erasedLocation[i]<paritySizeSRC){
                for (int f = 0; f < simpleParityDegree; f++) {
                    erasedValue[i]  = GF.add(erasedValue[i], dataRS[erasedLocation[i]*simpleParityDegree+f]);
                }
            }
        }
    }

    // if there is only a single lost node
    else if (erasedLocation.length == 1){
        // find its XOR group 
        if (erasedLocation[0]>=paritySizeSRC){
            singleErasureGroup = Math.ceil(((float)(erasedLocation[0]-paritySizeSRC+1))/((float)simpleParityDegree));
        }
        else{
            singleErasureGroup = erasedLocation[0]+1;
        }
        //System.out.println(Arrays.toString(dataRS));
        //and repair it
        for (int f = 0; f < simpleParityDegree; f++) {
            erasedValue[0]  = GF.add(erasedValue[0], dataRS[((int)singleErasureGroup-1)*simpleParityDegree+f]);
        }
        erasedValue[0] = GF.add(erasedValue[0],data[(int)singleErasureGroup-1]);
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

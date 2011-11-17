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

import java.io.OutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

public class ReedSolomonDecoder extends Decoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.ReedSolomonDecoder");
  private ErasureCode[] reedSolomonCode;
  private long decodeTime;
  private long waitTime;
  ExecutorService parallelDecoder;
  Semaphore decodeOps;

  public ReedSolomonDecoder(
    Configuration conf, int stripeSize, int paritySize, int simpleParityDegree) {	
    super(conf, stripeSize, paritySize,simpleParityDegree);    
    this.reedSolomonCode = new ReedSolomonCode[parallelism];
    for (int i = 0; i < parallelism; i++) {
      reedSolomonCode[i] = new ReedSolomonCode(stripeSize, paritySize, simpleParityDegree);
    }
    decodeOps = new Semaphore(parallelism);
    LOG.info("MAHESH initialized ReedSolomonDecoder" +
    		" with simpleParityDegree = "+simpleParityDegree);
  }

  @Override
  protected void fixErasedBlockImpl(
      FileSystem fs, Path srcFile,
      FileSystem parityFs, Path parityFile,
       long blockSize, long errorOffset, long limit,
       OutputStream out, Progressable reporter, boolean lightDecoder) throws IOException {
    FSDataInputStream[] inputs = new FSDataInputStream[stripeSize + paritySize];
    int[] erasedLocations = buildInputs(fs, srcFile, parityFs, parityFile, 
                                        errorOffset, inputs, lightDecoder);
    int blockIdxInStripe = ((int)(errorOffset/blockSize)) % stripeSize;
    int erasedLocationToFix = paritySize + blockIdxInStripe;

    // Allows network reads to go on while decode is going on.
    int boundedBufferCapacity = 2;
    parallelDecoder = Executors.newFixedThreadPool(parallelism);
    ParallelStreamReader parallelReader = new ParallelStreamReader(
      reporter, inputs, bufSize, parallelism, boundedBufferCapacity, blockSize);
    parallelReader.start();
    decodeTime = 0;
    waitTime = 0;
    try {
      writeFixedBlock(inputs, erasedLocations, erasedLocationToFix,
                      limit, out, reporter, parallelReader, lightDecoder);
    } finally {
      // Inputs will be closed by parallelReader.shutdown().
      parallelReader.shutdown();
      LOG.info("Time spent in read " + parallelReader.readTime +
        ", decode " + decodeTime + " wait " + waitTime);
      parallelDecoder.shutdownNow();
    }
  }

  protected int[] buildInputs(FileSystem fs, Path srcFile, 
                              FileSystem parityFs, Path parityFile,
                              long errorOffset, FSDataInputStream[] inputs, 
                              boolean lightDecoder)
      throws IOException {
    LOG.info("Building inputs to recover block starting at " + errorOffset);
    try {
      FileStatus srcStat = fs.getFileStatus(srcFile);
      long blockSize = srcStat.getBlockSize();
      long blockIdx = (int)(errorOffset / blockSize);
      long stripeIdx = blockIdx / stripeSize;
      LOG.info("FileSize = " + srcStat.getLen() + ", blockSize = " + blockSize +
               ", blockIdx = " + blockIdx + ", stripeIdx = " + stripeIdx);
      ArrayList<Integer> erasedLocations = new ArrayList<Integer>();
      //ArrayList<Integer> erasedLocationsLight = new ArrayList<Integer>();
      int[] locationsToFetch = new int[paritySize+stripeSize];      
      for(int i=0;i<paritySize+stripeSize;i++) {
    	  locationsToFetch[i] = 0;    	  
      }
    	  
      if(lightDecoder) {
    	  LOG.info("Starting with Light Decoder");
    	  for (int i = paritySize; i < paritySize + stripeSize; i++) {    		  
    		  long offset = blockSize * (stripeIdx * stripeSize + i - paritySize);
    		  if (offset == errorOffset) {
		          LOG.info(srcFile + ":" + offset +
		              " is known to have error, adding zeros as input " + i);
		          inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
		              offset + blockSize));
		          erasedLocations.add(i);
    		  }
    	  }
    	  int[] erasedLocationsArray = new int[erasedLocations.size()];
    	  for(int i = 0; i < erasedLocations.size(); i++) {
    		  erasedLocationsArray[i] = erasedLocations.get(i);
    	  }
    	  LOG.info("LALALA:  erasedLocations = "+convertToString(erasedLocationsArray));   
    	  blocksToFetch(erasedLocationsArray,locationsToFetch);
    	  LOG.info("LALALA:  locationsToFetch = "+convertToString(locationsToFetch));   	
    	  for(int i = 0; i < locationsToFetch.length; i++) {
    		  FSDataInputStream in;    		    
    		 
    		  if(i<paritySize) {
    			  long offset = blockSize * (stripeIdx * paritySize + i);
    			  if(locationsToFetch[i]>0) {
	    			  in = parityFs.open(
	    			          parityFile, conf.getInt("io.file.buffer.size", 64 * 1024));
	    			  LOG.info("Adding " + parityFile + ":" + offset + " as input " + i);
	    			  in.seek(offset);
	        		  inputs[i] = in;
    			  }
    			  else {
    				  inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
    			              offset + blockSize));
    				  
    				  LOG.info("Adding zeros as input "+i+" for offset "+offset);
    			  }
    		  }else {
    			  long offset = blockSize * (stripeIdx * stripeSize + i - paritySize);
    			  if(locationsToFetch[i]>0) {
	    			  in = fs.open(
	    			            srcFile, conf.getInt("io.file.buffer.size", 64 * 1024));
	    			  LOG.info("Adding " + srcFile + ":" + offset + " as input " + i);
	    			  in.seek(offset);
	        		  inputs[i] = in;
    			  }
    			  else {
    				  inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
    			              offset + blockSize));
    				  
    				  LOG.info("Adding zeros as input "+i+" for offset "+offset);
    			  }
    		  }  		  
    		  
    	  }
    	  LOG.info("Light decoder finished");
    	  //throw new IOException("LIGHT DECODER FAILED");
      }
      else {
    	  // Do the heavy decoding!
    	  LOG.info("Begin Heavy Decoder");
	      // First open streams to the parity blocks.    	  
	      for (int i = 0; i < paritySize; i++) {
	        long offset = blockSize * (stripeIdx * paritySize + i);
	        FSDataInputStream in = parityFs.open(
	          parityFile, conf.getInt("io.file.buffer.size", 64 * 1024));
	        in.seek(offset);
	        LOG.info("Adding " + parityFile + ":" + offset + " as input " + i);
	        inputs[i] = in;
	      }
	      // Now open streams to the data blocks.      
	      for (int i = paritySize; i < paritySize + stripeSize; i++) {
	        long offset = blockSize * (stripeIdx * stripeSize + i - paritySize);
	        if (offset == errorOffset) {
	          LOG.info(srcFile + ":" + offset +
	              " is known to have error, adding zeros as input " + i);
	          inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
	              offset + blockSize));
	          erasedLocations.add(i);
	        } else if (offset > srcStat.getLen()) {
	          LOG.info(srcFile + ":" + offset +
	                   " is past file size, adding zeros as input " + i);
	          inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
	              offset + blockSize));
	        } else {
	          FSDataInputStream in = fs.open(
	            srcFile, conf.getInt("io.file.buffer.size", 64 * 1024));
	          in.seek(offset);
	          LOG.info("Adding " + srcFile + ":" + offset + " as input " + i);
	          inputs[i] = in;
	        }
	      }
	      LOG.info("Heavy Decoder finished");
      }
      if (erasedLocations.size() > paritySize) {
        String msg = "Too many erased locations: " + erasedLocations.size();
        LOG.error(msg);
        throw new IOException(msg);
      }
      int[] locs = new int[erasedLocations.size()];
      for (int i = 0; i < locs.length; i++) {
        locs[i] = erasedLocations.get(i);
      }
      return locs;
    } catch (IOException e) {
      RaidUtils.closeStreams(inputs);
      throw e;
    }

  }

  private String convertToString(int[] array) {
	  String str =""+array[0];	  
	  for(int i=1;i<array.length;i++) 
		  str = str+", "+array[i];
	  
	  return str;
  }
  /**
   * Decode the inputs provided and write to the output.
   * @param inputs array of inputs.
   * @param erasedLocations indexes in the inputs which are known to be erased.
   * @param erasedLocationToFix index in the inputs which needs to be fixed.
   * @param limit maximum number of bytes to be written.
   * @param out the output.
   * @throws IOException
   */
  void writeFixedBlock(
          FSDataInputStream[] inputs,
          int[] erasedLocations,
          int erasedLocationToFix,
          long limit,
          OutputStream out,
          Progressable reporter,
          ParallelStreamReader parallelReader, 
          boolean lightDecoder) throws IOException {

    LOG.info("Need to write " + limit +
             " bytes for erased location index " + erasedLocationToFix);
    int[] tmp = new int[inputs.length];
    int[] decoded = new int[erasedLocations.length];
    // Loop while the number of written bytes is less than the max.
    for (long written = 0; written < limit; ) {
      erasedLocations = readFromInputs(
        inputs, erasedLocations, limit, reporter, parallelReader, lightDecoder);
      if (decoded.length != erasedLocations.length) {
        decoded = new int[erasedLocations.length];
      }

      int toWrite = (int)Math.min((long)bufSize, limit - written);

      int partSize = (int) Math.ceil(bufSize * 1.0 / parallelism);
      try {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < parallelism; i++) {
          decodeOps.acquire(1);
          int start = i * partSize;
          int count = Math.min(bufSize - start, partSize);
          parallelDecoder.execute(new DecodeOp(
            readBufs, writeBufs, start, count,
            erasedLocations, reedSolomonCode[i]));
        }
        decodeOps.acquire(parallelism);
        decodeOps.release(parallelism);
        decodeTime += (System.currentTimeMillis() - startTime);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while waiting for read result");
      }


      for (int i = 0; i < erasedLocations.length; i++) {
        if (erasedLocations[i] == erasedLocationToFix) {
          out.write(writeBufs[i], 0, toWrite);
          written += toWrite;
          break;
        }
      }
    }
  }

  int[] readFromInputs(
          FSDataInputStream[] inputs,
          int[] erasedLocations,
          long limit,
          Progressable reporter,
          ParallelStreamReader parallelReader, 
          boolean lightDecoder) throws IOException {
    ParallelStreamReader.ReadResult readResult;
    try {
      long start = System.currentTimeMillis();
      readResult = parallelReader.getReadResult();
      waitTime += (System.currentTimeMillis() - start);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for read result");
    }

    // Process io errors, we can tolerate upto paritySize errors.
    for (int i = 0; i < readResult.ioExceptions.length; i++) {
      IOException e = readResult.ioExceptions[i];
      if (e == null) {
        continue;
      }
      if (e instanceof BlockMissingException) {
        LOG.warn("Encountered BlockMissingException in stream " + i);
      } else if (e instanceof ChecksumException) {
        LOG.warn("Encountered ChecksumException in stream " + i);
      } else {
        throw e;
      }

      // Found a new erased location.
      //TODO: Shouldn't it be >
      if (erasedLocations.length == paritySize) {
        String msg = "Too many read errors";
        LOG.error(msg);
        throw new IOException(msg);
      }

      // Add this stream to the set of erased locations.
      int[] newErasedLocations = new int[erasedLocations.length + 1];
      for (int j = 0; j < erasedLocations.length; j++) {
        newErasedLocations[j] = erasedLocations[j];
      }
      newErasedLocations[newErasedLocations.length - 1] = i;
      erasedLocations = newErasedLocations;
    }
    readBufs = readResult.readBufs;
    LOG.info("LALALA:  In readInputs, erasedLocations = "+convertToString(erasedLocations));
    
    if(lightDecoder&&(erasedLocations.length>1))
    	throw new IOException("LIGHT DECODER FAILED");    
       
    return erasedLocations;
  }

  class DecodeOp implements Runnable {
    byte[][] readBufs;
    byte[][] writeBufs;
    int startIdx;
    int count;
    int[] erasedLocations;
    int[] tmpInput;
    int[] tmpOutput;
    ErasureCode rs;
    DecodeOp(byte[][] readBufs, byte[][] writeBufs,
             int startIdx, int count, int[] erasedLocations,
             ErasureCode rs) {
      this.readBufs = readBufs;
      this.writeBufs = writeBufs;
      this.startIdx = startIdx;
      this.count = count;
      this.erasedLocations = erasedLocations;
      this.tmpInput = new int[readBufs.length];
      this.tmpOutput = new int[erasedLocations.length];
      this.rs = rs;
    }

    public void run() {
      try {
        performDecode();
      } finally {
        decodeOps.release();
      }
    }

    private void performDecode() {
      for (int idx = startIdx; idx < startIdx + count; idx++) {
        for (int i = 0; i < tmpOutput.length; i++) {
          tmpOutput[i] = 0;
        }
        for (int i = 0; i < tmpInput.length; i++) {
          tmpInput[i] = readBufs[i][idx] & 0x000000FF;
        }
        rs.decode(tmpInput, erasedLocations, tmpOutput);
        for (int i = 0; i < tmpOutput.length; i++) {
          writeBufs[i][idx] = (byte)tmpOutput[i];
        }
      }
    }
  }
  
  public void blocksToFetch(int[] erasedLocation, int[] locationsToFetch) {
  
		int paritySizeRS = (simpleParityDegree)*(paritySize+stripeSize)/(simpleParityDegree+1)-stripeSize;
		int paritySizeSRC = paritySize-paritySizeRS;
		int flagErased = 0;
		int locationsLength = 0;
		double singleErasureGroup;


		if (erasedLocation.length == 0) {
			return;
		}
		// initialize the locations to fetch to
		for (int i = 0; i<paritySizeSRC+paritySizeRS+stripeSize; i++){
			locationsToFetch[i]=0;
		}
		//first check if the is a single failure
		if (erasedLocation.length == 1){
			//find the simpleXOR group that the erased block is a member of
			if (erasedLocation[0]>=paritySizeSRC){
				singleErasureGroup = Math.ceil(((float)(erasedLocation[0]-paritySizeSRC+1))/((float)simpleParityDegree));
			}
			else{
				singleErasureGroup = erasedLocation[0]+1;
			}
			// indicate the blocks that need to be communicated
			for (int f = 0; f < simpleParityDegree; f++) {//parityRS and stripe blocks
				locationsToFetch[paritySizeSRC+((int)singleErasureGroup-1)*simpleParityDegree+f]=1;
			}
			locationsToFetch[(int)singleErasureGroup-1]=1;//simpleXOR block
			for (int i = 0; i<paritySizeSRC+paritySizeRS+stripeSize; i++){
				if (i ==erasedLocation[0])
				{
					locationsToFetch[i]=0;
				}
			}

		}
		else if (erasedLocation.length > 1){
			for (int i = 0; i < stripeSize+paritySizeRS; i++){
				for (int j = 0; j<erasedLocation.length; j++){
					if(erasedLocation[j]==paritySizeSRC+i){
						flagErased = 1;
					}
				}
				if (flagErased==0){
					locationsToFetch[paritySizeSRC+i]=1;
					locationsLength++;
					if (locationsLength==stripeSize)
						return;
				}
				else
				{
					flagErased = 0;
				}
			}

		}
	}
  
  public void blocksToFetch2(int[] erasedLocation, int[] locationsToFetch) {
	  for(int i=0;i<paritySize+stripeSize;i++) {
		  if(i!=erasedLocation[0])
			  locationsToFetch[i] = 1;
		  else
			  locationsToFetch[i] = 0;
	  }
  }
  
  
  
}

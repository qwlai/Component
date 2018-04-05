# CS3223 Project

Project Group Members:<a name="group"></a>

* Lai Qi Wei (A0160137N)
* Oh Han Yi (A0160210E)
* Chan Jun Yuan (A0160133X)

# Table of Contents
1. [Introduction](#introduction)
2. [Implementation](#implementation)
    1. [Block Nested Loop Join](#blocknested)
    2. [Sort Merge Join](#sortmerge)
    3. [Distinct](#distinct)
    4. [2 Phase Optimization](#2PO)

## Introduction<a name="introduction"></a>
This document provides some of the features implemented in the query processing engine.

## Implementation<a name ="implementation"></a>
This section shows the implementation of some features.

### Block Nested Loop Join<a name = "blocknested"></a>
This section shows how `Block Nested Loop Join`(BNL) is implemented. 

The `open()` phase of BNL is very similar to normal `NestedJoin`, except an additional ArrayList `leftBlockTuples`, a block to contain the leftTuples, is initialized in this phase. Once `open()` is done successfully, the program will move to the `next()` phase.

In the `next()` phase, BNL will perform `loadLeftBlock()` to add the leftTuples into the ArrayList `leftBlockTuples` to simulate a block of leftTuples. It will add (Buffer - 2) number of leftTuples into this `leftBlockTuples` ArrayList because 2 buffers are used to as a input buffer to load the rightTuples and the output buffer (`outbatch`), therefore the remaining buffers are used to store the leftTuples. For each block `leftBlockTuples`, the entire right Table is scanned tuple by tuple, and matching results will be written to the output buffer. If `outbatch` is full, output the page of output tuples and end that instance of `next()`.

### Sort Merge Join<a name = "sortmerge"></a>
This section shows how `Sort Merge Join`(SMJ) is implemented. 

### Distinct<a name = "distinct"></a>
This section shows how `Distinct` results are filtered. 

### 2 Phase Optimization<a name = "2PO"></a>
This section shows how `2 Phase Optimization`(2PO) algorithm is implemented. 


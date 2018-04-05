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
        1. [ExternalSort](#externalsort)
    3. [Distinct](#distinct)
    4. [2 Phase Optimization](#2PO)

## Introduction<a name="introduction"></a>
This document provides some of the features implemented in the query processing engine.

## Implementation<a name ="implementation"></a>
This section shows the implementation of some features.

### Block Nested Loop Join<a name = "blocknested"></a>
This section shows how `Block Nested Loop Join`(**BNL**) is implemented. 

The `open()` phase of **BNL** is very similar to normal `NestedJoin`, except an additional ArrayList `leftBlockTuples`, a block to contain the leftTuples, is initialized in this phase. Once `open()` is done successfully, the program will move to the `next()` phase.

In the `next()` phase, **BNL** will perform `loadLeftBlock()` to add the leftTuples into the ArrayList `leftBlockTuples` to simulate a block of leftTuples. It will add (Buffer - 2) number of leftTuples into this `leftBlockTuples` ArrayList because 2 buffers are used to as a input buffer to load the rightTuples and the output buffer (`outbatch`), therefore the remaining buffers are used to store the leftTuples. For each block `leftBlockTuples`, the entire right Table is scanned tuple by tuple, and matching results will be written to the output buffer. If `outbatch` is full, output the page of output tuples and end that instance of `next()`.

### Sort Merge Join<a name = "sortmerge"></a>
This section shows how `Sort Merge Join`(**SMJ**) is implemented. **SMJ** is implemented by first doing an `Externalsort` on the two tables before scanning through them for matching results and outputting them.

In the `open()` phase of **SMJ**, after calculating the number of tuples per batch and getting the left and right attributes, we will then run `ExternalSort()` on both the left and right table as shown below.

    leftRelation = new ExternalSort(left, numBuff, leftIndex, leftRunName);
    rightRelation = new ExternalSort(right, numBuff, rightIndex, rightRunName);
      
#### External Sort <a name = "externalsort"></a>
In the first phase of the `ExternalSort`, the tuples are first loaded into memory using `loadTuplesIntoMainMemory()`. The tuples in the main memory is then sorted using the code as shown below.

    /**
     * Sorts main memory tuples
     */
    private void sortMainMem() {
        if (isDistinct && attributeList != null) {
            Collections.sort(mainMemTuples, new Comparator<Tuple>() {
                @Override
                public int compare(Tuple o1, Tuple o2) {
                    Vector attList = attributeList;

                    int finalComparison = 0;
                    for (int i = 0; i < attList.size(); i++) {
                        int index = table.getSchema().indexOf((Attribute) attList.get(i));
                        int result;
                        result = Tuple.compareTuples(o1, o2, index);
                        finalComparison = result;
                        if (result == 0) {
                            mainMemTuples.remove(o1);
                        } else {
                            break;
                        }
                    }
                    return finalComparison;
                }
            });
        } else {
            Collections.sort(mainMemTuples, ((t1, t2) -> Tuple.compareTuples(t1, t2, joinIndex)));
        }
    }

After the tuples in the main memory is sorted, `writeSortedRun()` will then write out attributes we are interested in into sorted runs batch by batch. `writeSortedRun()` is performed until either all tuples have been written into sorted runs or if the main memory no longer has any more tuples.

In the second phase of the `ExternalSort`, `mergeSortedRun()` will then be used to merge these sorted runs back into 1 sorted file using `numBuff - 1` buffers. `mergeSortedRun()` will be completed when the number of sorted runs become 1.

    /**
     * Phase 2: Merge sorted runs
     * Pages to merge = numBuffer - 1;
     */
    private void mergeSortedRuns() {
        int numOfUsableBuffers = numBuff - 1;
        int readInCurrentRun = 0;
        int writeOutRunCounter = 0;
        while (runNum != 1) { // last run not completed yet
            /** Merge all runs in current pass */
            while (readInCurrentRun != runNum) {
                for (int i = 0; i < numOfUsableBuffers; i++) {
                    if (!readSortedRun(readInCurrentRun)) {
                        break;
                    }
                    readInCurrentRun++;
                }
                sortMainMem();
                writeSortedRuns(writeOutRunCounter);
                writeOutRunCounter++;
            }
            readInCurrentRun = 0;
            writeOutRunCounter = 0;

            /** Number of sorted runs left */
            runNum = (int) Math.ceil((double) runNum / numOfUsableBuffers);
            close();
        }
    }

`ExternalSort` is then completed after these 2 phases and will return back to **SMJ's** `open()`. Next, two priority queues (`leftPQ` and `rightPQ`) which are sorted by index are created. A `tupleStack` is also initialized.

    leftPQ = new PriorityQueue<>((t1, t2) -> Tuple.compareTuples(t1, t2, leftIndex));
    rightPQ = new PriorityQueue<>((t1, t2) -> Tuple.compareTuples(t1, t2, rightIndex));
    tupleStack = new Stack<>();

In the `next()` of **SMJ**, 2 buffers will be used for the right relation and the output buffer, so `(Buffer - 2)` pages will be used to load pages from the left relation. The priority Queue will then be populated by the tuples sorted by join index. Below is the `loadLeftBlock()` codes to show how it is added.

    /**
     * Loads left block, M - 2 buffer used for left block
     * @Exception EOFException when no more batch object to be read
     */
    private void loadLeftBlock() {
        for (int i = 0; i < (numBuff - 2); i++) {
            try {
                Batch batch = (Batch) inLeft.readObject();
                if (batch != null) {
                    for (int j = 0; j < batch.size(); j++) {
                        leftPQ.add(batch.elementAt(j));
                    }
                }
            } catch (EOFException e) {
                try { // 1 load all into buffer
                    if (isFirstBlock) {
                        leftTuple = leftPQ.poll();
                        isFirstBlock = false;
                    }

                    inLeft.close();
                    hasLoadLastLeftBlock = true;
                    File f = new File(leftRunName + "0");
                    f.delete();
                } catch (IOException io) {
                   //error codes
                }
            } catch (ClassNotFoundException cnfe) {
                //error codes
            } catch (IOException io) {
                return;
            }
        }

        if (isFirstBlock) {
            leftTuple = leftPQ.poll();
            isFirstBlock = false;
        }
    }
    
After loading the left block and right batch into memory, while `leftPQ` and `rightPQ` is not empty, the tuples will then compared and matching tuples will be added to the output buffer. Upon matching tuples, we will push the `rightTuple` onto the `tupleStack` and poll it from the `RightPQ`.

    /**
     * Compare left and right tuples, join tuples if they match the condition
     */
    private void compareWithRightRelation() {

        if (rightTuple == null || leftTuple == null) {
            return;
        }

        int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftIndex, rightIndex);

        if (comparison == 0) { // matching join value, poll right
            Tuple outTuple = leftTuple.joinWith(rightTuple);
            outBatch.add(outTuple);

            tupleStack.push(rightTuple);
            rightTuple = rightPQ.poll();

            if (outBatch.isFull()) { // after adding, check if is full
                return;
            }
        } else if (comparison > 0) { // left > right, progress right
            tupleStack.push(rightTuple);
            rightTuple = rightPQ.poll();
        } else { // left < right progress left
            if (leftPQ.peek() != null) {
                /** If next tuple is the same, right has progress more than left, restore */
                if (Tuple.compareTuples(leftPQ.peek(), leftTuple, leftIndex) == 0) {
                    undoPQ();
                }
            }
            leftTuple = leftPQ.poll();
        }
    }

We also use `undoPQ` in cases where join condition is not set on the primary key, which can result in duplicates in the left relation. This allows us to revert back to the previous state by adding it back into the Priority Queue.
    
    /**
     * Handle case where join condition is not set on pkey, duplicates may occur on left relation
     * Undo PQ to previous state
     */
    private void undoPQ() {
        if (rightTuple != null) {
            tupleStack.push(rightTuple);
        }

        while (!tupleStack.isEmpty() && Tuple.compareTuples(leftTuple, tupleStack.peek(), leftIndex, rightIndex) <= 0) {
            rightPQ.add(tupleStack.pop());
        }

        rightTuple = rightPQ.poll();
    }

### Distinct<a name = "distinct"></a>
This section shows how `Distinct` results are filtered. 

### 2 Phase Optimization<a name = "2PO"></a>
This section shows how `2 Phase Optimization`(**2PO**) algorithm is implemented. 

The **2PO** algorithm we implemented consists of using the default Iterative Improvement (**II**) algorithm given to us, coupled with another randomized algorithm called Simulated Annealing (**SA**) algorithm. We will first use **II** algorithm to choose the plan with the lowest IO cost among the random plans within a fixed number of iterations, then pass that plan and its associated cost to the **SA** algorithm to further optimize the plan. We use `TEMP`, `count` and `equilibrium` to act as checks for this algorithm. 

* `TEMP` refers to the temperature and it is used to determine the probability of accepting uphill moves and number of times the outer loop is performed. 
* `equilibrium` refers to the counter to determine when a stage ends.
* `count` refers to the number of consecutive moves, where the optimizer will end if after a specific movies, there is still no better plans.

A stage is referred to the inner while loop. Below is the **SA** algorithm code we implemented for **2PO** algorithm.

    public Operator getOptimizedPlanSA() {
        //Initialize plan with II
        Operator initPlan = getOptimizedPlan();
        ... other codes ...

        int MINCOST = Integer.MAX_VALUE;      //S0 cost
        double TEMP = 0.1 * initCost;         //initial temperature
        int equilibrium = 16 * numJoin;
        int count = 0;
        while (TEMP >= 1 && count < 4) {      //1 entire inner loop is 1 stage
            while (equilibrium > 0) {   
                //System.out.println("--------while--------");
                Operator initPlanCopy = (Operator) initPlan.clone();
                Operator neighbor = getNeighbor(initPlanCopy);
                pc = new PlanCost();
                int neighborCost = pc.getCost(neighbor);
                //System.out.println("--------------------------neighbor---------------");
                //Debug.PPrint(minNeighbor);

                int delta = neighborCost - initCost;

                if (delta <= 0) {
                    // neighborCost - initCost <= 0 means downhill, set initial plan to neighbor
                    initPlan = neighbor;
                    initCost = neighborCost;
                } else {
                    // neighborCost - initCost > 0 means uphill, set initial plan to neighbor with probability
                    double probability = Math.exp((-1) * (delta / TEMP));
                    Random random = new Random();
                    if (random.nextDouble() <= probability) {
                        initPlan = neighbor;
                        initCost = neighborCost;
                    }
                }
                // if initial cost is < min cost, update min cost
                if (initCost < MINCOST) {
                    //System.out.println("Count is resetted");
                    count = 0;
                    System.out.println("-----------SA new Minimum Plan-------------");
                    finalPlan = initPlan;
                    MINCOST = initCost;
                    Debug.PPrint(initPlan);
                    System.out.println(initCost);
                } else if (initCost == MINCOST) {
                    count++;
                    //System.out.println("current count is :" + count);
                    if (count >= 4)
                        break;
                }
                equilibrium--;  //decrement equilibrium since stage ends
            }

            TEMP = TEMP * 0.95;
            equilibrium = 16 * numJoin;
        }

        System.out.println("\n");
        System.out.println("-----------------Final Plan to Execute--------------\n");
        if (finalPlan != null) {
            Debug.PPrint(finalPlan);
            System.out.println("  " + MINCOST);
            return finalPlan;
        } else {
            System.out.println();
            Debug.PPrint(initPlan);
            System.out.println("  " + initCost);
            return initPlan;
        }
    }

A stage (inner loop) ends normally when `equilibrium` decrements to 0. At the end of each stage, `TEMP` is reduced to 95% and `equilibrium` is reset back to its default value. When the `TEMP` reaches 1, the optimizer will break out of the loop and return the current optimal plan. Within a stage, if the optimizer picks a plan that has the same cost, `count` will be incremented. When `count` reaches 4, the optimizer will also break out of the loop and return the current optimal plan.

The difference between **II** and **SA** algorithm is that **II** will not pick a plan which does an uphill move while **SA** uses probability to determine whether to take an uphill move or not. Sometimes, a better plan can be found by taking uphill moves followed by downhill moves. **SA** algorithm provides us with more flexibility in terms of looking through such plans and this is the main reason why we chose **2PO** which incoporates both the **II** and **SA** algorithm.

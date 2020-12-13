package algo1;

/** 									**Algorithm One**
 * 
 * The first algorithm attempts to efficiently calculate the sum aggregates of the joins of N number of tables
 * t1,t2,...tn without materializing the join or using more than O(K^2) memory where K is the number of keys in the database.
 * 
 * Consider the tables below and notice that a natural join would, for example, produce the row {2,3,4,5,0,5} in the
 * {A,B,C,D,E,F} schema. 
 * 
 * 
 * 			A 	B 	C			C	D			D	E	F
 * 			2   3   1			1	5			3	9	12
 * 			1	12	1			2	12			5	0	5
 * 			3	0	2			3	3			5	1	1	
 * 			3	2	3			4	5			5 	18	2
 * 			6	3	4			4	8			8	15	9
 * 												12	1	1
 * 
 * Instead of materializing this row, we can have iterators which point to the rows from each table 
 * that produce the row {2,3,1,5,0,5}
 * 
 * 
 *  		A 	B 	C			C	D			D	E	F
 * 	--It1-> 2   3   1	--It2->	1	5			3	9	12
 * 			1	12	1			2	12	--It3->	5	0	5
 * 			3	0	2			3	3			5	1	1	
 * 			3	2	3		    4	5			5 	18	2
 * 			6	3	4			4	8			8	15	9
 * 												12	1	1
 * 
 * We use this strategy in the algorithm, we have a iterator linearly scan the first table;
 * for each row the first iterator passes, the second iterator, one row at a time, finds all matching rows with the same keys
 * in the second table. For each such row the second iterator finds matching the row the first iterator is on,
 *  the third iterator finds, one at a time, matching rows in the third table that have the same values for keys that are
 *  in the first two tables (a natural join).  Each time the last iterator (in this case iterator 3), successfully finds a matching
 *  row, we compute all sum aggregates for that row. 
 *  
 *  NOTICE THAT THE TABLES ABOVE ARE SORTED ON THE KEYS THEY ARE JOINING INTO THE RESULT TABLE ON. 
 *  THIS ALLOWS ITERATORS TO USE BINARY SEARCH TO QUICKLY FIND WHERE IN THE TABLE MATCHING ROWS LIE.
 *  
 *  
 *  
 *  Consider an iterator t, that iterates through the rows of table t. The iterator has three functions,
 *  
 *  synchronize(): To find rows in table T with the same values for common keys with tables 1 to T-1, 
 *  the iterator must be synchronized with the iterators of previous tables so that it knows which keys to join on.
 *  When we increment iterator t-1, we must synchronize() iterator t so that it is aware of any changes.
 *  The synchronize function has various optimizations, some depending on which iterator implementation is being used
 *  (we have a few implementations like JoinIterator and SortedIterator). For instance, if iterator t-1
 *  is incremented but that next row has the same key values for the join keys with table t then the iterator does not need to
 *  look for new matching rows as it remembers where the rows matching the last join keys are located. Similarly,  
 *  if the join keys do not change and there was no matching rows last time, the iterator knows there will be no matches this time
 *  We attempt to sort the tables in such as way so that they are sorted first on their join keys (so the iterators
 *  can use binary search to find matching rows) and records with the same join keys are sorted by future join keys so that
 *  future tables will have incoming rows with repeating join keys which allows the iterators to optimize. The Synchronize()
 *  function precomputes the value of the next row that the iterator will point to when increment() is called. If no such value
 *  exists it points to EMPTY. We precompute the next value so that if hasNext() is called more than once in a row,
 *  the iterator does not have to search for the next matching row each time.
 * 
 *  
 *  hasNext(): Returns true if and only if there is another matching row for the current join keys and hence the iterator can be 
 *  incremented. If hasNext() returns false, that means the iterators has supplied all the matching rows for those join keys
 *  and so iterators before t must be incremented so that new join keys are provided. 
 *  
 *  increment(): Moves the iterator to point to the next row that satisfies the current join keys and precomputes
 *  the next matching row after that, if one exists.
 *  
 *  When we reach iterator t, we call hasNext() to see if iterator t has any more matching rows for the common keys (join keys)
 *  with the rows iterators 1 to t-1 point to. If hasNext() returns true, we increment the iterator t to point to this next matching
 *  row. Now we must "join" this new "virtual" result row with the remaining tables t+1 through N. We do this by
 *  now going to the next iterator t+1, calling synchronize() and seeing if it has a match for that row by calling hasNext() and then
 *  increment if it is successful and so on. If the iterator t did not have a matching row (hasNext() returned false),
 *  we must go back to iterator t-1 and increment it so that iterator t can be provided with a new row to join with
 *  since it ran out of rows for the old join key. If iterator t-1's hasNext() also returns false (iterator
 *  t-1 ran out of rows to join with the rows pointed to by iterators 1 to t-2 on), we must go back to iterator t-2 and try
 *  to increment it and so on...
 *  
 *  
 *  As we said before, each table T sorted by a composite key. The columns that take precedence in the the composite key
 *  are those the table T uses to join with the result rows from tables 1 to T-1 on. Further, the columns are
 *  sorted in the same order that we use to join the columns. The reason for all this, as we said before,
 *  is so that iterator T can easily find matching rows with the rows pointed to by iterators 1 to T-1 with
 *  binary search. Because we are not allowed to use extra memory for an index, this is our way of "simulating"
 *  an index with an in place sort and binary search. We have implemented a "dumbIterator" which does not take 
 *  advantage of the sortedness and it makes the program perform horribly even for small datasets (many minutes just 
 *  for dataset 2). This shows how important  using indices or binary search is. 
 *  
 *  The columns taking less precedence are columns that will act as future join keys into later tables as we have
 *  several optimizations that take advantage of sorted incoming join keys. For instance, if iterator T
 *  incoming result rows from tables 1 to T-1 are sorted by the join keys it is joining with them on it can 
 *  perform something similar to a sort_merge join and hence would not even need to do binary search to find rows.
 *  
 *  Three main types of iterators...Our implementation figures out which one should be used for different iterators
 *  
 *  1) StartIterator: The iterator of the first table has the easy job of just linearly going to the
 *  next row of the table from row 1 to the last row each time we increment it. The first table is sorted
 *  on the join keys of the next table in the join order so that the next table can sort itself on the same keys and use
 *  sort merge iterators
 *  
 *  2) Sort Merge Iterator: Assumes that incoming rows are sorted on the join keys 
 *  of the table and the incoming rows and hence uses sort merge type strategy to avoid using binary search to find rows
 *  since they will be one after another. The second table will always be able to use the sort merge iterator because
 *  we always sort the first table on the second table's join keys first since the first table does not have to sort itself
 *  on own its join keys since it does not have any since it is the first table. If the third table is joining on the same
 *  keys as the second table, it can also use sort merge iterator and so on. The speed up is not significant 
 *  from the JoinIterator however since binary search is already very quick
 *  
 *  3) Join Iterator) The general purpose iterator which makes no assumptions about incoming tuples and sorts its
 *  table on the join keys with incoming rows so that it can find matching tuples with binary search
 *  
 *  The algorithm needs to be provided only tables to join (basic data structure with array of rows and a schema)
 *  it figures out the join order and which iterators to use itself. A user of the algorithm can 
 *  optionally also provide an 2D of strings which tell the algorithm which aggregates to compute if they
 *  only want some aggregates. For instance, if we only want SUM(A*B) and SUM(B*C) and no others. Otherwise, the algorithm provides
 *  all the aggregates.
 *  
 */



import java.util.Arrays;

import table.HashIterator;
import table.JoinIterator;
import table.Join_Utility;
import table.SortMergeJoinIterator;
import table.StartIterator;
import table.Table;
import table.TableIterator;

public class JoinAlg {

	private final Table[] 		tables;
	private final Join_Utility  data;
	private final int 			totalKeys;
	private final double[][] 	aggregates;
	private final int[][] 		keysToAggregateOn;
	private final double[] 		keyValues;
	
	private final TableIterator[] iterators;
	
	private JoinAlg(Table[] init_tables, String[][] strAggs){
		
		this.data       = new Join_Utility(init_tables, strAggs);
		this.keysToAggregateOn  = data.getKeysToAggregateOn();
		this.tables = data.getJoinOrder();
		
		this.iterators 	 = new TableIterator[tables.length];
		this.totalKeys   = data.getAttributes().size();
		this.aggregates  = new double[totalKeys][totalKeys];
		this.keyValues   = new double[totalKeys];
		setUpIterators();

	
	}
	
	private void setUpIterators(){
		
		iterators[0] = new StartIterator(data, keyValues);
		int[] firstJoinKeys = null;
		
		if(tables.length > 0){
			iterators[1] = new SortMergeJoinIterator(data, 1,keyValues);
			firstJoinKeys = data.getJoinKeys()[1];
		}
		
		int it = 2;
		
		while(it < tables.length && Arrays.equals(data.getJoinKeys()[it], firstJoinKeys)){
				iterators[it] = new SortMergeJoinIterator(data, it, keyValues);
				++it;
		}
		
		while(it < tables.length){
			iterators[it] = new JoinIterator(data, it, keyValues);
			++it;
		}
	
	}
	
	private void printResult(){

		for(int k1 = 0; k1 < totalKeys; ++k1)
			for(int k2 = k1; k2 < totalKeys; ++k2)
				System.out.println("SUM(" + data.getAttribute(k1) + "*" + data.getAttribute(k2) + ") = " + aggregates[k1][k2]);
			
	}
	
	private void printResultWithSelectAggregates(){
		
		for(int[] ag: keysToAggregateOn)
			System.out.println("SUM(" + data.getAttribute(ag[0]) + "*" + data.getAttribute(ag[1]) + ") = " + aggregates[ag[0]][ag[1]]);

	}
		
	private void join(){

		int curr = 0; //The rightmost iterator that is not empty 
		TableIterator it;

		while(curr != 0 || iterators[0].hasNext()){ //We are done when the first table's iterator ends
			it = iterators[curr];	

			if(it.hasNext()){
				it.increment();
				if(curr+1 == iterators.length) //Is this the last iterator, then compute aggregates
					computeAggregates();
				else
					iterators[++curr].synchronize(); //Refresh the next iterator now that we have updated the keys 

			}
			else
				curr--;

		} 
		
	}

	private void computeAggregates(){
			
		if(keysToAggregateOn != null) {computeSelectAggregates(); return;}
		
		for(int k1 = 0; k1 < totalKeys; ++k1)
			for(int k2 = k1; k2 <totalKeys; ++k2)
				aggregates[k1][k2] += keyValues[k1]*keyValues[k2];
		
	
	}
	
	
	private void computeSelectAggregates(){
		
		for(int[] ag: keysToAggregateOn)
			aggregates[ag[0]][ag[1]] += keyValues[ag[0]]*keyValues[ag[1]];
			
		}


	public static double[][] runWithoutPrint(Table[] init_tables){
		JoinAlg algo = new JoinAlg(init_tables, null);
		algo.join();
		return algo.aggregates;
		
	}

	
	public static double[][] run(Table[] init_tables){
		JoinAlg algo = new JoinAlg(init_tables, null);
		algo.join();
		algo.printResult(); 
		return algo.aggregates;
		

		
	}
	//Same algorithm as the ones before 
	//Including the variable aggregates tells the algorithm to only compute aggregates for those variables
	public static double[][] runWithoutPrint(Table[] init_tables, String[][] aggregates){
		JoinAlg algo = new JoinAlg(init_tables, aggregates);
		algo.join();
		return algo.aggregates;
		
	}

	
	public static double[][] run(Table[] init_tables, String[][] aggregates){
		JoinAlg algo = new JoinAlg(init_tables, aggregates);
		algo.join();
		algo.printResultWithSelectAggregates();
		return algo.aggregates;

	}
	
	
	

}

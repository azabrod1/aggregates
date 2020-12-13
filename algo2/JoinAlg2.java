package algo2;

/** Algorithm 2:
 * 
 * In the first algorithm we compute all n(n+1)/2 aggregates each time we successfully increment the last iterator 
 * (each time we increment the last iterator, the rows the iterators point to represent a tuple in the result table had
 * we materialized the join)
 * 
 * 
 * This algorithm has two main improvements
 * 
 * 1) Rather than computing all the aggregates SUM(XY) at the end of the join (when the last iterator is incremented),
 * we push the aggregation up the join and compute SUM(XY) in the table T such that both aggregates X and Y do not appear in earlier tables
 * In other words, we can push the aggregates up the join until we reach the earliest table in the join order such that X and Y do not appear in
 * earlier tables
 * 
 * 2) If a table T < N increments its iterator and the join keys on which tables T+1,...N join the result table do not change, 
 * 	there is no point in recomputing what happens with later iterators and computing the aggregates, we already know the exact same thing will happen
 * since the join keys remain the same. Thus, the algorithm is able to "simulate" the results of joins when the join keys do not change when an
 * iterator is incremented.
 * 
 * Both of these improvements offer dramatic speed ups of the algorithm
 * 
 */


import java.util.Arrays;

import table.DumbJoinIterator;
import table.JoinIterator;
import table.Join_Utility;
import table.SortMergeJoinIterator;
import table.StartIterator;
import table.Table;
import table.TableIterator;

public class JoinAlg2 {

	private final Table[]         tables;
	private final Join_Utility    data;
	private final int 			  totalKeys;
	private final double[][]      aggregates;
	private final int[][] 		  keysToAggregateOn;
	private final double[][]      sumBuffer;
	private final double[][]      aggBuffer;
	private final TableIterator[] iterators;
	private		  long 			  clock;    //The number of joined rows (not-materialized) computed
	private final double[]        keyValues; //Value of each key at any point of the join
	private final long[]          snapShot; //snapshot[it] is the last "time" iterator it was incremented where time refers to 
    										// our discrete definition of time

	
	private JoinAlg2(Table[] init_tables, String[][] strAggs){
		
		this.data      		 = new Join_Utility(init_tables, strAggs);
		this.tables          = data.getJoinOrder();
		
		this.iterators       = new TableIterator[tables.length];
		this.totalKeys       = data.getAttributes().size();
		this.aggBuffer  	 = new double[tables.length][totalKeys*totalKeys];
		this.aggregates      = new double[totalKeys][totalKeys];
		
		this.sumBuffer       = new double[tables.length][totalKeys];
		this.snapShot        = new long[tables.length];
		
		this.keyValues       = new double[totalKeys];
		this.keysToAggregateOn = data.getKeysToAggregateOn();
		Arrays.fill(keyValues, -23.1);
		Arrays.fill(snapShot, -1);
		

		
		data.prepareDataForAlg2(); 
		
		setUpIterators();

		
	}
	
	private void setUpIterators(){
		
		iterators[0] = new StartIterator(data, keyValues);
		int[] firstJoinKeys = null;
		
		if(tables.length > 0){
			iterators[1] = new SortMergeJoinIterator(data, 1, keyValues);
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
		
		int curr = 0; //The rightmost iterator that can be incremented 
		TableIterator it;

		while(curr > 0 || iterators[0].hasNext()){ //We are done when the first table's iterator ends
			
			//Handle the last iterator separately
			if(curr + 1 == iterators.length){ 
				it = iterators[iterators.length - 1];
				if(it.hasNext()){
					it.increment(); ++clock; 
					computeRightMostAggregates(); 
				}
				else
					curr--;
			
				continue;
			}

		    it = iterators[curr];

			
			if(it.hasNext()){
				
				if(snapShot[curr] == -1){
					snapShot[curr] = clock;
					it.increment(); iterators[++curr].synchronize();
				}

				else{

					int startRow = it.currentRow();

					while(true){
						
						boolean keysChanged = it.increment(); //iterators[curr+1].synchronize();
						
						if(keysChanged){ //Join keys changed, so compute aggregates for the rows with the same key
							iterators[curr+1].synchronize();
							computeAggregates(curr, startRow, it.currentRow() - 1);
							snapShot[curr] = clock;
							curr++;
							//Now we move on to next iterator to compute next part of join
							break;
						}

						//The join keys for the next table stayed the same so we can reuse the pre-computed aggregates in the buffer

						if(!it.hasNext()){ 
							computeAggregates(curr, startRow, it.currentRow());
							snapShot[curr--] = -1;
							//Now we move on to next iterator to compute next part of join
							break;
						}
					}


				}

			}

			else{

				if(snapShot[curr] != -1)
					computeAggregates(curr, it.currentRow(), it.currentRow());
				
				snapShot[curr--] = -1; 
				
			}
			
		}
		
		if(snapShot[0] != -1) //Because of how the algorithm is structured, we may not account for last row of first table so
			computeAggregates(0, iterators[0].currentRow(), iterators[0].currentRow()); //we do so here
		
		//Accumulate result in array to return
		for(int k1 = 0; k1 < totalKeys; ++k1)
			for(int k2 = k1; k2 < totalKeys; ++k2)
				if(k1<=k2) 
					aggregates[k1][k2] = aggBuffer[0][k1*totalKeys+k2];
		
	
		
		
	}
	
	

	/**
	 * During the algorithm we pass along intermediate results (aggregates and sums)
	 * from the last table in the join order to the first. The intermediate results
	 * are stored in buffers (each iterator has a buffer associated with it to store aggregates)
	 * 
	 * 
	 * Table 1  <------   Table 2 <----- Table 3 <----- ...... <----- Table N
	 * 
	 * The computeAggregates() function for iterator t takes the intermediate results in the next buffer (t+1)
	 * and uses them to update the aggregates in its own buffer using also the rows in its own table 
	 * 
	 * 
	 * If we currently are at table t and the next row of table t has the same values for the keys 
	 * that will be used to join the result table with tables later in the join order, it means we do not need
	 * to recompute aggregates for each row where the keys do not change. We simply multiply the aggregates by the number
	 * of matching rows (startRow - endRow +1). 
	 * 
	 * The input StartRow is the row we originally computed aggregates by looking at further tables.
	 * endRow is the last row which has the same join keys as startRow and hence the last row for which we 
	 * can "skip" computation of intermediate results by using the aggregates accumulated with startRow 
	 * 
	 * 
	 * 
	 */
	
	
	private void computeAggregates(int t, int startRow, int endRow){

		double timeElapsed = clock - snapShot[t];

		if(timeElapsed == 0) //No rows were successfully joined, so nothing to do 
			return;
		
		double[] row;
		int[] sameTableAggs      = data.getSameTableAggs(t);
		int[] sameTableAggCols	 = data.getSameTableCols(t);
		int[] firstAppearingKeys = data.getFirstAppearingKeys(t);
		int[] firstAppeaingCols  = data.getFirstAppearingCols(t);
		double[] buf       	     = aggBuffer[t];
		double[] sumBuf          = sumBuffer[t];
		int[]  mixedAggs         = data.getMixedAggs(t);
		int[]  writeMixed        = data.getWriteMixedAggs(t);
		double[] nextSumBuf      = sumBuffer[t+1];
		double[] nextBuf       	 = aggBuffer[t+1];
		int[]    aggsLater       = data.getLaterAggs(t);


		Table table = tables[t];


		for(int r = startRow; r <= endRow; ++r){
			row = table.getRow(r);

			/**1) Compute aggregates in the form of SUM(XY) where X and Y are two columns that appear for the first time in
			 * the join order in table T. We have that SUM(XY) = (x1*y1 + x2y2 +...xnyn)*timeElapsed where we refer to the start row as row 1
			 * and the last row as row n. We multiply by time elapsed because that is how many tuples match one of the rows of table t that
			 * we are aggregating on
			 *  */
			for(int k = 0; k < sameTableAggCols.length; k+=2)	
				buf[sameTableAggs[k/2]] += row[sameTableAggCols[k]]*row[sameTableAggCols[k+1]]*timeElapsed;

			/**2) Compute linear sums in the form SUM(X) where X is a column in this table that never appears 
			 * earlier in the join order
			 */

			for(int k = 0; k < firstAppearingKeys.length; ++k)
				nextSumBuf[firstAppearingKeys[k]] += row[firstAppeaingCols[k]];
			
		}


		/**3) Compute aggregates in the form SUM(XA) where X is a key that appears in the join
		 * order for the first time in this table (never earlier) and A is a key that always appears 
		 * after table t in the join order
		 * 
		 */
		
		
		for(int k = 0; k < mixedAggs.length; k+=2)
			buf[writeMixed[k/2]] += nextSumBuf[mixedAggs[k]]*nextSumBuf[mixedAggs[k+1]];
		
		/** 4)) Update sums of the form SUM(A) where A is any key that does not appear in
		 *  table t or any table earlier in the join order
		 *  
		 *  We need to do this adjustment because we sometimes "skip" portions of the joins where the key does
		 *  not change and hence we know the result without any computations. 
		 *  
		 *  NEW_SUM(A) = OLD_SUM(A)*(1+ROWS_SKIPPED)
		 * 
		 */
		
		final int numRows = 1 + endRow - startRow;
		
		for(int key: data.getKeysAfter(t)){
			sumBuf[key] += numRows*nextSumBuf[key];
			nextSumBuf[key] = 0;	//Reset next buffer for reuse
		}
		
		
		/** 5) Update Aggregates of the form SUM(AB) where both A and B appear later in the join order. The updates
		 *  are necessary to account for the times we skip portions of joins 
		 * 
		 * NEW_SUM(AB) = OLD_SUM(AB)*(1+ROWS_SKIPPED)
		 * 
		 */
		
		
		for(int agg : aggsLater){
			buf[agg] += numRows*nextBuf[agg]; 
			nextBuf[agg] = 0; //Reset next buffer for reuse
		}
		
		/** 6) multiply linear sums of the form SUM(X) where X is a column appearing for the first time in the join order in this
		 * table. We have UPDATED_SUM(X) = SUM(X)*TIME_ELAPSED since the time elapsed is the number of matching tuples
		 * with the key value X since all the tuples calculated since the last snapShot were done with the same join key
		 * 
		 */
		
		for(int key : firstAppearingKeys){
			sumBuf[key] += nextSumBuf[key]*timeElapsed;
			nextSumBuf[key] = 0;
		}
		
		clock += (endRow - startRow)*timeElapsed; //increase clock to make up for the joins we "skipped"
	
	}
	
	
	/**The compute aggregates function for the last table in the join order
	 * is just a subset of the computeAggregates() function used for the other tables because
	 * we do not need to consider aggregates in later tables since it is the last table in the join order
	 */
	
	
	private void computeRightMostAggregates(){
		final int t = tables.length - 1;
		double[] row = tables[t].getRow(iterators[t].currentRow());
		double[] buf       	     = aggBuffer[t];
		double[] sumBuf          = sumBuffer[t];
		int[] sameTableAggs      = data.getSameTableAggs(t);
		int[] sameTableAggCols	 = data.getSameTableCols(t);
		int[] firstAppearingKeys = data.getFirstAppearingKeys(t);
		int[] firstAppeaingCols  = data.getFirstAppearingCols(t);

		/**1) Compute aggregates in the form of SUM(XY) where X and Y are two columns that appear for the first time in
		 * the join order in table T     */
		for(int k = 0; k < sameTableAggCols.length; k+=2)	
			buf[sameTableAggs[k/2]] += row[sameTableAggCols[k]]*row[sameTableAggCols[k+1]];

		/**2) Compute linear sums in the form SUM(X) where X is a column in this table that never appears 
		 * earlier in the join order
		 */

		for(int k = 0; k < firstAppearingKeys.length; ++k)
			sumBuf[firstAppearingKeys[k]] += row[firstAppeaingCols[k]];
		
	}
	
	
	
	public static double[][] runWithoutPrint(Table[] init_tables){
		JoinAlg2 algo = new JoinAlg2(init_tables, null);
		algo.join();
		return algo.aggregates;
		
	}

	
	public static double[][] run(Table[] init_tables){
		JoinAlg2 algo = new JoinAlg2(init_tables, null);
		algo.join();
		algo.printResult();
		return algo.aggregates;
		
	}
	//Same algorithm as the ones before 
	//Including the variable aggregates tells the algorithm to only compute aggregates for those variables
	public static double[][] runWithoutPrint(Table[] init_tables, String[][] aggregates){
		JoinAlg2 algo = new JoinAlg2(init_tables, aggregates);
		algo.join();
		return algo.aggregates;
		
	}

	
	public static double[][] run(Table[] init_tables, String[][] aggregates){
		JoinAlg2 algo = new JoinAlg2(init_tables, aggregates);
		algo.join();
		algo.printResultWithSelectAggregates();
		return algo.aggregates;
		
	}
	
}


package algo1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import table.JoinIterator;
import table.Join_Utility;
import table.SortMergeJoinIterator;
import table.StartIterator;
import table.Table;
import table.TableIterator;

/**Naive Algorithm that uses same optimizations as the first algorithm but uses the same optimizations
 * as the first algorithm (like sorting tables to find the keys quickly) but it materializes the result
 * before computing aggregates. We decided to do many of the same optimizations we
 * did for the first algorithm because we are interested in comparing our implementation to 
 * a naive standard commercial database. We have no doubt that the database would optimize joins with
 * either sorting, indexing or hashing and hence we want a "fair" comparison so we keep optimizations such as sorting
 * 
 *
 */


public class JoinAlgNaive {

	private final Table[] 		tables;
	private final Join_Utility  data;
	private final int 			totalKeys;
	private final double[][] 	aggregates;
	private final int[][] 		keysToAggregateOn;
	private final double[] 		keyValues;
	
	private final TableIterator[] iterators;
	
	private final List<double []>    joinResult; //The result of the join


	
	
	private JoinAlgNaive(Table[] init_tables, String[][] strAggs){
		
		this.data       = new Join_Utility(init_tables, strAggs);
		this.tables = data.getJoinOrder();
		
		this.iterators 	 = new TableIterator[tables.length];
		this.totalKeys   = data.getAttributes().size();
		this.aggregates  = new double[totalKeys][totalKeys];
		this.keyValues   = new double[totalKeys];
		setUpIterators();

		
		//In case the user does not want all aggregates but only specific ones
		if(strAggs != null){
			keysToAggregateOn = new int[strAggs.length][2];
			for(int a = 0; a < strAggs.length; ++a){
				int[] toAgg = {data.getKey(strAggs[a][0]), data.getKey(strAggs[a][1])};
				keysToAggregateOn[a] = toAgg;
			}
		}
		else keysToAggregateOn = null;
		
		joinResult = new ArrayList<double[]>();
		
		
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
	
	static int clock = 0;
	

	private void join(){

		int curr = 0; //The rightmost iterator that is not empty 
		TableIterator it;

		while(curr != 0 || iterators[0].hasNext()){ //We are done when the first table's iterator ends
			it = iterators[curr];	

			if(it.hasNext()){
				it.increment();
				if(curr+1 == iterators.length) //Is this the last iterator, then compute aggregates
					materializeRow();				
				else
					iterators[++curr].synchronize(); //Refresh the next iterator now that we have updated the keys 

			}
			else
				curr--;

		} 
		
		computeAggregates();
		
	}
	
	private void materializeRow(){
		++clock;
		if(joinResult.size() == 1000000){
			computeAggregates(); joinResult.clear();
		}
		joinResult.add(Arrays.copyOf(keyValues, keyValues.length)); //Materialize Row

	}
	
	

	private void computeAggregates(){
		
		if(keysToAggregateOn != null) {computeSelectAggregates(); return;}
		
		for(double[] row: joinResult)
			for(int k1 = 0; k1 < totalKeys; ++k1)
				for(int k2 = k1; k2 <totalKeys; ++k2)
					aggregates[k1][k2] += row[k1]*row[k2];
		
	}
	

	private void computeSelectAggregates(){

		for(double[] row: joinResult)
		for(int[] ag: keysToAggregateOn)
			aggregates[ag[0]][ag[1]] += row[ag[0]]*row[ag[1]];
			
		
	}
	
	
	
	public static double[][] runWithoutPrint(Table[] init_tables){
		JoinAlgNaive algo = new JoinAlgNaive(init_tables, null);
		algo.join();
		return algo.aggregates;
		
	}

	
	public static double[][] run(Table[] init_tables){
		JoinAlgNaive algo = new JoinAlgNaive(init_tables, null);
		algo.join();
		algo.printResult(); System.err.println(clock);
		return algo.aggregates;
		

		
	}
	//Same algorithm as the ones before 
	//Including the variable aggregates tells the algorithm to only compute aggregates for those variables
	public static double[][] runWithoutPrint(Table[] init_tables, String[][] aggregates){
		JoinAlgNaive algo = new JoinAlgNaive(init_tables, aggregates);
		algo.join();
		return algo.aggregates;
		
	}

	
	public static double[][] run(Table[] init_tables, String[][] aggregates){
		JoinAlgNaive algo = new JoinAlgNaive(init_tables, aggregates);
		algo.join();
		algo.printResultWithSelectAggregates();
		return algo.aggregates;
		
	}
	
	
	

}

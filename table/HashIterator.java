
/**
 * The Join Iterator is used to iterate through the columns of tables that are being joined into the  
 * main join result. The iterator stores the current row it is on and the next row that it will
 * point to if it is incremented
 * 
 * The iterator is used like this:
 * 
 * synchronize(): Synchronizes the iterator with the state of the iterators before hand. In other words,
 * we look to what the values of the keys we are joining on are currently by looking where the 
 * iterators are at the tables that determine those key's values. Afterward, we do a binary search
 * to find the first row of the table (if it exists, otherwise nextRow = EMPTY) that matches the new 
 * join key values. This function should be used every time the iterators of tables joined before this one
 * are incremented
 * 
 * hasNext(): Informs the user if there is another row that matches the current values of the join keys.
 * 
 * increment(): sets the currentRow to the value of nextRow which was computed beforehand.
 * 				The function assumes that hasNext() was called first to check if more valid rows
 * 				matching the join keys exist. Afterward, the increment function precomputes the value of
 * 				the next row the iterator will take if it is incremented again.
 * 
 * The important thing is that the iterator achieves efficiency similar to the sortMerge iterator
 * without assuming anything about incoming tuples' sort order. The iterator does this by sorting its
 * table based on the keys it is joining into the result table on and uses binary search to find corresponding tuples
 *
 *
 */
package table;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HashIterator implements TableIterator {

	private final Table[]     tables;
	private final Table       relation;
	private final int[]       joinCols; //Columns in the table we are joining on  
	private final int[]       joinKeys;
	private final double[]    keyValues;
	
	private static final int  NULL = -3;  	 
	private static final int  EMPTY = -2;    //The iterator has no rows left to produce which match current key

													//As we can remember what the first row corresponding to the key is 
	private  int              currRow  = EMPTY;
	private Integer           nextRow  = EMPTY;
	private       double[]    currKeys;
	private final int[] 	  firstAppearingKeys;
	private final int[] 	  firstAppearingCols;
	private final boolean[]   joinKeysAfter;
	private final Map<JoinKey, Integer> matchingRows = new HashMap<JoinKey, Integer>();
	
	private final static boolean DIRTY = true; 
	private final static boolean CLEAN = false;


	
	public HashIterator(Join_Utility data, int ID, double[] keyValues ){
		this.tables    = data.getJoinOrder();
		this.relation  = tables[ID];
		
		//The join keys that this iterator will be joining the table on 
		int[] joinKeys = data.getJoinKeys()[ID];
	
		this.joinCols    = new int[joinKeys.length]; 		
		
		//Identify which columns of the table we are joining on
		for(int k = 0; k < joinKeys.length; ++k)
			joinCols[k] = relation.keyToCol(joinKeys[k]);

		//Sort the table on the join columns. It is crucial to sort on the columns in the right order
		
		relation.sort(data.getSortCols(ID), joinCols);
 

		this.currKeys  = new double[joinKeys.length];
		
		this.keyValues = keyValues;
		this.firstAppearingKeys = data.getFirstAppearingKeys(ID);
		this.firstAppearingCols = data.getFirstAppearingCols(ID);
		this.joinKeysAfter      = data.getJoinKeysAfter(ID);
		this.joinKeys      		= data.getJoinKeys(ID);


	}
	
private class JoinKey{
		
		final double[] keys;
		
	 public JoinKey(double[] keys) {
			this.keys = keys;
		}
		
		@Override
		public int hashCode(){
		
			return Arrays.hashCode(keys);
		}
		
		@Override
		public boolean equals(Object other){
			if(this == other) return true;
			
			JoinKey that = (JoinKey) other;
			
			return Arrays.equals(this.keys, that.keys);
		}
		
	}
	

	/** This function synchronizes the iterator's keys with their current values and finds the next row, if it exists,
	 *  whose value the iterator will take if it is incremented. Returns true if the value of the join keys have changed 
	 */
	public void synchronize(){
		
		//double[] x = currKeys;
		
		currKeys = new double[joinKeys.length];
		
		
		for(int k = 0; k < joinKeys.length; ++k)
			currKeys[k] = keyValues[joinKeys[k]];
		
		JoinKey curr = new JoinKey(currKeys);
		
		//System.out.println(Arrays.toString(currKeys) + "   " + Arrays.toString(x));

		nextRow = matchingRows.get(curr);
		
		
		//if(nextRow != null){
		//	System.err.println("hashed!!");
		//	return;
		//}
		
		
		//Otherwise search for it
		//We can do a binary search since we sort the table on the keys
		double[] searchKey = new double[relation.numCols()];

		for(int k = 0; k < currKeys.length; ++k)
			searchKey[joinCols[k]] = currKeys[k];


		nextRow = relation.binarySearch(searchKey);
		

		if(nextRow < 0)
			nextRow = EMPTY; 
		
		matchingRows.put(curr, nextRow);
			
		return;

	}
	
	/**Is there another row matching the current value of the join keys?*/
	
	public boolean hasNext(){
		
		return nextRow != EMPTY;
	}
	
	/** The increment operation does two things: 1) Moves the current row forward to the 
	 * value of "next row" which was computed beforehand
	 * 
	 * 2) Recomputes nextRow, the value the iterator will take the next time it is incremented. Because the table is sorted
	 * on the join keys, incrementing an iterator, if possible, just means going to the row directly below the previous one
	 * 
	 * 
	 */
	public boolean increment(){

		currRow = nextRow;
		
		//Update the values of the keys that appear in this table (do not need to update the ones appearing earlier 
		//in join order)

		boolean toReturn = CLEAN;
		int i = 0; double value;
		
		for(int key :firstAppearingKeys){
			value = relation.valueAt(firstAppearingCols[i++], currRow);
			if(keyValues[key] != value){
				if(joinKeysAfter[key]) 
					toReturn = DIRTY;
				
				keyValues[key] = value;
				
			}
			
		}

		nextRow += 1; //Relation is sorted, so we know the next matching tuple is just below this row

		if(nextRow == relation.getSize()){
			nextRow = EMPTY; return toReturn;
		}
		
		//If  would be next row's join columns do not match the join keys, the iterator is done
		for(int k = 0; k < joinCols.length; ++k){
		
			if(relation.valueAt(joinCols[k], nextRow) != currKeys[k]){
				nextRow = EMPTY;
				return toReturn;
			}
		}
		
		return toReturn;
	}

	@Override
	public int currentRow() {

		return  currRow;
	}

	
}



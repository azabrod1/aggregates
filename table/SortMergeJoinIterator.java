package table;

/** The sort merge iterator takes advantage of a situation where the stream of incoming tuples is sorted on the same join keys
 * 	they would join with the Iterator's table on. Then, the iterator knows that each incoming tuple will match with the same
 * 	rows the last one matched with if the join key values remained the same or a row shortly after the last row.
 * 	The iterator essentially performs a sort-merge join but without materializing the actual join
 * 
 * If incoming tuples cannot be trusted to be sorted on the join keys they are merging with this table on, 
 * then we use the JoinIterator which sorts its corresponding table based on its join keys and uses binary search
 * to find corresponding tuples instead of sort merge. 
 * 
 * 
 *
 */

public class SortMergeJoinIterator implements TableIterator {


	private final Table[]     tables;
	private final Table       relation;
	private final int[]       joinCols; //Columns in the table we are joining on  
	private final int[] 	  joinKeys;
	
	private static final int  NULL = -3;  	 
	private static final int  EMPTY = -2;    //The iterator has no rows left to produce which match current key

	private  int 			  lastStartRow = NULL; //If the key does not change from last time, we do not need do binary search again
													//As we can remember what the first row corresponding to the key is 
	private int               currRow  = EMPTY;
	private int               nextRow  = EMPTY;
	private final double[]    currKeys;
	private boolean           foundMatch = false;
	private final double[]    keyValues;
	private final int[] 	  firstAppearingKeys;
	private final int[] 	  firstAppearingCols;
	private final boolean[]   joinKeysAfter;
	
	final static boolean DIRTY = true; 	final static boolean CLEAN = false;


	
	public SortMergeJoinIterator(Join_Utility data, int ID,  double[] keyValues){
		
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

		this.currKeys  = new double[joinKeys.length]; //current value of the join keys

		this.keyValues = keyValues;
		this.firstAppearingKeys = data.getFirstAppearingKeys(ID);
		this.firstAppearingCols = data.getFirstAppearingCols(ID);
		this.joinKeysAfter      = data.getJoinKeysAfter(ID);
		this.joinKeys           = data.getJoinKeys(ID);
		

	}

	/** This function synchronizes the iterator's keys with their current values and finds the next row, if it exists,
	 *  whose value the iterator will take if it is incremented.
	 * @return returns true if and only if the value of the keys changed from last time
	 */
	public void synchronize(){
		
		boolean keysChanged = false;

		for(int k = 0; k < joinKeys.length; ++k){

			double keyVal = keyValues[joinKeys[k]];

			if(keyVal != currKeys[k]){
				currKeys[k]  = keyVal;
				keysChanged  = true;
			}
		}
		
		
		if(lastStartRow == NULL) //Initially we start at the top of the table 
			nextRow = 0;
		else if(lastStartRow == EMPTY) //Since the tables are sorted, we know once we reach the end, we stay there
			return;
		else if(foundMatch){      //Did we find matching tuples before the last synchronization?
			
			if(!keysChanged){ //If we found matches last time with the same key, we just go back to the last start row
				nextRow = lastStartRow; 
				return;
			}
			else{ //Found match and keys changed from last time
				
				foundMatch = false;  //reset for next time
				nextRow = currRow + 1; //If we found matches last time and have a new key now, because the keys are sorted
							     		//we can just start one row after the last match since keys are sorted 
				
				if(nextRow == relation.getSize()){ //Did we run out of rows?
					lastStartRow = EMPTY; nextRow = EMPTY;
					return;
				}
			}
		}
		
		else if(!keysChanged){ //If the keys did not change, and there was no match last time, there still wont be one this time
			return;
		}
		else
			nextRow = lastStartRow;
		
		
		//Is nextRow smaller, equal to, or larger than the keys
		int difference = compareNextRowWithKeys();
		
		while(difference < 0){ //Increment nextRow while it is too small to match with current keys
			++nextRow;
			if(nextRow == relation.getSize()){
				nextRow = EMPTY;
				lastStartRow = EMPTY;
				return;
			}
			difference = compareNextRowWithKeys();
		}
		
		lastStartRow = nextRow;
		
		if(difference == 0)
			foundMatch = true;
		
		if(difference > 0)
			nextRow = EMPTY;
		
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
		//in the join order)
		
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

	/**Returns negative if the current row is less than the current keys, positive if it is greater and 0 if equal */
	private int compareNextRowWithKeys(){
			
    	double difference;
    	for(int comp = 0; comp < joinCols.length; ++comp){
    		difference = relation.valueAt(joinCols[comp], nextRow) - currKeys[comp];
    		if(difference != 0)
    			return (int) difference;
    		
    	}
    	return 0; 
    }
	
	
}

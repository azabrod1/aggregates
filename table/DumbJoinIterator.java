package table;

/**
 * Same as Join iterator but does not sort the table on the join keys and hence must perform linear scan instead of binary search
 * to retrieve matching records. Performs very poorly, takes several minutes even for the second data set
 * 
 *
 */


public class DumbJoinIterator implements TableIterator {

	private final Table[]     tables;
	private final Table       relation;
	private final int[]       joinCols; //Columns in the table we are joining on 
	private final int[]       joinKeys;
	private final boolean[]   joinKeysAfter;
	
	private static final int  NULL = -3;  	 
	private static final int  EMPTY = -2;    //The iterator has no rows left to produce which match current key

	private  int 			  lastStartRow = NULL; //If the key does not change from last time, we do not need do binary search again
													//As we can remember what the first row corresponding to the key is 
	private  int              currRow  = EMPTY;
	private int               nextRow  = EMPTY;
	private final double[]    currKeys;
	
	private final double[]    keyValues;
	private final int[] 	  firstAppearingKeys;
	private final int[] 	  firstAppearingCols;
	
	final static boolean DIRTY = true; 	final static boolean CLEAN = false;

	
	
	public DumbJoinIterator(Join_Utility data, int ID, double[] keyValues){
		this.tables    = data.getJoinOrder();
		this.relation  = tables[ID];

		//The join keys that this iterator will be joining the table on 
		int[] joinKeys = data.getJoinKeys()[ID];
	
		this.joinCols    = new int[joinKeys.length]; 
		
		//Identify which columns of the table we are joining on
		for(int k = 0; k < joinKeys.length; ++k)
			joinCols[k] = relation.keyToCol(joinKeys[k]);


		this.currKeys  = new double[joinKeys.length];
		
		this.keyValues = keyValues;
		this.firstAppearingKeys = data.getFirstAppearingKeys(ID);
		this.firstAppearingCols = data.getFirstAppearingCols(ID);
		this.joinKeys           = data.getJoinKeys(ID);
		this.joinKeysAfter      = data.getJoinKeysAfter(ID);

	}

	/** This function synchronizes the iterator's keys with their current values and finds the next row, if it exists,
	 *  whose value the iterator will take if it is incremented.
	 */
	public void synchronize(){
		
		boolean keysChanged = false;

		for(int k = 0; k < joinCols.length; ++k){
			double keyVal = keyValues[joinKeys[k]];

			if(keyVal != currKeys[k]){
				currKeys[k]  = keyVal;
				keysChanged  = true;
			}
		}

		//If the key has not changed, we already know the start row
		if(!keysChanged && lastStartRow != NULL){
			nextRow = lastStartRow; 
			return;
		}

		//Otherwise search for it
		double[] searchKey = new double[relation.numCols()];

		for(int k = 0; k < currKeys.length; ++k)
			searchKey[joinCols[k]] = currKeys[k];


		nextRow = relation.findAfter(searchKey, -1, joinCols);

		if(nextRow < 0)
			nextRow = EMPTY; 

		lastStartRow = nextRow;
		
		return;

	}

	/**Is there another row matching the current value of the join keys?*/
	
	public boolean hasNext(){
		
		return nextRow != EMPTY;
	}
	
	/** The increment operation does two things: 1) Moves the current row forward to the 
	 * value of "next row" which was computed beforehand
	 * 
	 * 2) Recomputes nextrow, the value the iterator will take the next time it is incremented. Because the table is sorted
	 * on the join keys, incrementing an iterator, if possible, just means going to the row directly below the previous one
	 * 
	 * 
	 */
	public boolean increment(){

		currRow = nextRow;
		
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
		

		// search for the row after that
		double[] searchKey = new double[relation.numCols()];

		for(int k = 0; k < currKeys.length; ++k)
			searchKey[joinCols[k]] = currKeys[k];
		
		
		int nextRowAfter = relation.findAfter(searchKey, nextRow, joinCols);
	
		if(nextRowAfter < 0) nextRow = EMPTY; 

		return toReturn;
	
	}

	@Override
	public int currentRow() {

		return  currRow;
	}

	
}

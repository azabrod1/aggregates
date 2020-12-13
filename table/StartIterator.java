package table;

/**Implements the Table Iterator that will iterate 
 * the first table in the join order. The first table is sorted on the same keys that it will 
 * join with the second table on so that the table's can be joined in a sort-merge fashion
 *
 * This is the simplest, because it just goes from the top to the bottom of the table
 */



public class StartIterator implements TableIterator {
	
	private final Table table;
	private int currRow = -1;
	private final double[]    keyValues;
	private final int[] 	  firstAppearingKeys;
	private final int[] 	  firstAppearingCols;
	private final boolean[]   joinKeysAfter;
	
	final static boolean DIRTY = true; 	final static boolean CLEAN = false;

	
	public StartIterator(Join_Utility data, double[] keyValues) {
		this.table = data.getJoinOrder()[0];
		
		
		
		//Sort first table by the keys that it will merge into second table with
		//So that we can do an (non-materialized) sort-merge join
		
		int[] keysWeJoinOn = data.getJoinKeys()[1]; 
		
		int[] joinCols    = new int[keysWeJoinOn.length]; 
		
		//Identify which columns of the table we are joining on
		for(int k = 0; k < joinCols.length; ++k)
			joinCols[k] = table.keyToCol(keysWeJoinOn[k]);
		
		//Sort the table on the join columns 
		table.sort(data.getSortCols(0), joinCols);
		
		this.keyValues = keyValues;
		this.firstAppearingKeys = data.getFirstAppearingKeys(0);
		this.firstAppearingCols = data.getFirstAppearingCols(0);
		this.joinKeysAfter      = data.getJoinKeysAfter(0);
		
	}
	
	
	//Takes the iterator to the beginning of the table
	@Override
	public void synchronize() {
		
		currRow = -1; 

	}

	//Are we on the last row yet?
	@Override
	public boolean hasNext() {
		
		return currRow+1 < table.getSize();
	}

	@Override
	public boolean increment() {
		++currRow;
		
		boolean toReturn = CLEAN;
		int i = 0; double value;
		
		for(int key :firstAppearingKeys){
			value = table.valueAt(firstAppearingCols[i++], currRow);
			if(keyValues[key] != value){
				if(joinKeysAfter[key]) toReturn = DIRTY;
				
				keyValues[key] = value;
				
			}
			
		}
		
		return toReturn;
			
	}

	@Override
	public int currentRow() {
		
		return currRow;
	}

}

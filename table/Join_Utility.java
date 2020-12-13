package table;

/**Utility class that creates a query plan and prepares datastructures (2d arrays) for the main algorithm*/


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Join_Utility {

	private final Map<Table, Map<Table, Set<Integer>>> commonKeys; 
	private final List<String>      	   attributes = new ArrayList<String>();
	private final Map<String, Integer> attributeToKey = new HashMap<String, Integer>(); //Maps attributes to their corresponding IDs

	private final Table[]     	    tables;
	private final Table[]           joinOrder;
	private final int[][]           joinKeys; //Keys that we are joining on at each step of the join
	private final int[][]           sortKeys; //Keys that we are joining on at each step of the join
	
	private final int[][]           firstAppearingKeys; //Keys that appear for the first time in the join order at table t
    private final int[][]           firstAppearingCols; //Columns corresponding the the firstAppearingKeys
    private final int[][] 			keysAfter;
	
    private final int[][]			aggsLater;			//Aggregates of keys always appearing later in join order than table t 
	private final int[][]    		mixedAggs;			//Aggregates of keys where one is in table T, one is later
	private final int[][]			writeMixedAggsIn;	//Precomputed location of where in buffer array to store intermediates
														//We precompute this to save time
	
	private final int[][]		    sameTableAggs;		//Aggregates of keys where both appear for first time in join order in
														//the same table
	
	private final int[][]		    sameTableAggCols;   //Columns in the table associated with the aggregates above
	
	private final boolean[][] 		joinKeysAfter;		//When we update a key, we check the update effects later join keys

	private final int				numKeys;
	private final int[][]  			keysToAggregateOn;

	
	public Join_Utility(Table[] tables, String[][] strAggs){
		
		this.tables = tables;
		String strAttribute;

		//Identify all attributes in database and give each one a unique ID which we refer to as a "key"
		
		for(Table table: tables){
			int[] newKeys = new int[table.getSchema().length];
			for(int a = 0; a < table.getSchema().length; ++a){
				strAttribute = table.getAttribute(a);
				if(!attributeToKey.containsKey(strAttribute)){ //Attribute has not yet been observed
					attributeToKey.put(strAttribute, attributes.size());
					attributes.add(strAttribute);					
				}
				newKeys[a] = attributeToKey.get(strAttribute);
			}
		
			table.setKeys(newKeys);
		}
		
		commonKeys = new HashMap<Table, Map<Table, Set<Integer>>>(); 
		
		Set<Integer> comKeys;

		for(Table right : tables){
			
			Map<Table, Set<Integer>> inCommon = new HashMap<Table, Set<Integer>>(); 

			for(Table left : commonKeys.keySet()){

				comKeys = new HashSet<Integer>();

				for(String atR : right.getSchema()){
					for(String atL : left.getSchema())
						if(atR.equals(atL)) comKeys.add(attributeToKey.get(atR));
				}

				inCommon.put(left, comKeys);
				commonKeys.get(left).put(right, comKeys);
			
			}
			commonKeys.put(right, inCommon);
		}
	

		//In case the user does not want all aggregates but only specific ones
		if(strAggs != null){
			keysToAggregateOn = new int[strAggs.length][2];
			for(int a = 0; a < strAggs.length; ++a){
				int[] toAgg = {Math.min(getKey(strAggs[a][0]), getKey(strAggs[a][1])), Math.max(getKey(strAggs[a][0]), getKey(strAggs[a][1])) };
				keysToAggregateOn[a] = toAgg;
			}
		}
		else keysToAggregateOn = null;
		
		joinKeys  = new int[tables.length][];
		sortKeys  = new int[tables.length][]; 
		this.numKeys = attributes.size();
		joinKeysAfter = new boolean[tables.length][numKeys];


		this.joinOrder = makeQueryPlan();
		generateSortCols();
		
		//For part 2
		firstAppearingKeys = new int[tables.length][];
		firstAppearingCols = new int[tables.length][];
		
		keysAfter   	 = new int[tables.length][];
			    
	    aggsLater    	 = new int[tables.length][];

		
		mixedAggs    	 = new int[tables.length][];	
		
		writeMixedAggsIn = new int[tables.length][];	
		
		sameTableAggs	 = new int[tables.length][];	
		
		sameTableAggCols = new int[tables.length][];
		
		calcFirstAppearingKeys();

	
	}
	
	public Set<Integer> getCommonKeys(Table t1, Table t2){
		
		return commonKeys.get(t1).get(t2);
	}
	
	public Set<String> getCommonAtts(Table t1, Table t2){

		Set<Integer> keys =  commonKeys.get(t1).get(t2);
		Set<String>  toReturn = new HashSet<String>();
		
		for(Integer key: keys){
			toReturn.add(attributes.get(key));
		}
		
		return toReturn;
		
	}

	public int getKey(String attribute){
		return attributeToKey.get(attribute);
	}
	
	public String getAttribute(int key){
		return attributes.get(key);
	}
	
	
	public int numCommon(Table t1, Table t2){
		
		return commonKeys.get(t1).get(t2).size();
	}
	
	
	public static Set<Integer> commonKeys(Set<Integer> schema, Table table){

		Set<Integer> commonK = new HashSet<Integer>();
		for(int att1 : schema){
			for(int att2: table.getKeys())
				if(att1 == att2)
					commonK.add(att1);
			
		}
		
		return commonK;
		
	}


	/**Function: makeQueryPlan()
	 * A function generates a join ordering by starting with the first table, picking a table that can be joined with it to form a 
	 * result table. Afterwards we pick a table that can be joined with the result table and so on. We assume that 
	 * in real life a better query planner would be available. The only optimization that exists here is that we try to
	 * start the join order with as many tables as possible that are joined on the same key, so we can do a sort merge type join	 */

	private Table[] makeQueryPlan(){

		Set<Table> unused    = new HashSet<Table>(); //Maintain a set of tables that are yet to be joined
		
		Table[] order    = new Table[tables.length];

		for(Table tbl : tables) unused.add(tbl);

		Set<Integer> currSchema = new HashSet<Integer>(); //The schema of the result table 

		Table first = unused.iterator().next(); 

		order[0] = first;          //Start the join with the first table 
		unused.remove(first);

		for(Integer att : first.getKeys()) currSchema.add(att);

		Set<Integer> lastCommonKeys = null; //The keys joined on last time; we try to join on same keys as long as we can
		//to optimize the joins
		
		joinKeys[0] = new int[0];

		for(int join = 1; join < tables.length; ++join){
			Table   currentBest = null; Table right = null;
			Iterator<Table> it = unused.iterator();
			
			while(it.hasNext()){
				right = it.next();
				Set<Integer> keysShared = commonKeys(currSchema, right);

				if(keysShared.size() > 0 && (currentBest == null || keysShared.equals(lastCommonKeys))){ //can only join tables with common attributes
				
					     if(keysShared.equals(lastCommonKeys)){ //Prefer to join table on same key as last time, so can optimize with sorting
					    	 currentBest = right;
					    	 break;
					     }
				    	 currentBest = right;
			}
				
				}
			
			lastCommonKeys = commonKeys(currSchema, currentBest);
									
			joinKeys[join] = new int[lastCommonKeys.size()];
			
			int x = 0;
			for(int key: lastCommonKeys)
				joinKeys[join][x++] = key;
			
			Arrays.sort(joinKeys[join]); //Always keep join keys in the same order for simplicity to make
										 //it easy to sort on the same keys in the same order always
			
			order[join] = currentBest;
			unused.remove(currentBest);
			
			for(Integer att : currentBest.getKeys())
				currSchema.add(att);
						
		}
						
		Set<Integer> setJoinKeysAfter = new HashSet<Integer>();
		
		
		for(int join = tables.length-1; join >= 0; --join){
			
			for(int key: setJoinKeysAfter)
				joinKeysAfter[join][key] = true;
			
			
			for(int key: joinKeys[join])
				setJoinKeysAfter.add(key);
						
		}
		
		return order;
	}
	

	
	/**This function determines the columns, and the order of the columns, that each table is sorted on
	 * 
	 * Most importantly, each table is sorted on the columns that it will be joined into the final result table on
	 * This way a binary search can be used to locate the rows matching the keys.
	 * 
	 * The tables are also sorted on future join columns in the order they appear in the query plan; this
	 * allows more optimization though it is not as important as the first part
	 * 
	 * Example, a table can be sorted on columns c6,c4,c2,c1 in that order because c6,c4 are used to join into the result table
	 * while columns c2 and c1 are used further down the join order to join the result table into other tables
	 * 
	 */
	
	private void generateSortCols(){
		
		List<Integer> sKeys = new ArrayList<Integer>();
		int column;
		Table table = joinOrder[0];

		//Determine the columns to sort on for the first table

		for(int jKey = 1; jKey < joinKeys.length; ++jKey){
			for(int key: joinKeys[jKey]){

				column = table.keyToCol(key);
				if(column != -1 && !sKeys.contains(column))
					sKeys.add(column);

			}

		}
		
		sortKeys[0] = new int[sKeys.size()];
		
		for(int sk = 0; sk < sKeys.size(); ++sk)
			sortKeys[0][sk] = sKeys.get(sk);
		
		sKeys.clear();

		for(int t = 1; t < joinOrder.length; ++t){
			
			table = joinOrder[t];
			
			//Determine the columns to sort on

			for(int jKey = t; jKey < joinKeys.length; ++jKey){
				for(int key: joinKeys[jKey]){

					column = table.keyToCol(key);
					if(column != -1 && !sKeys.contains(column))
						sKeys.add(column);

				}

			}
			
			sortKeys[t] = new int[sKeys.size()];
			
			for(int sk = 0; sk < sKeys.size(); ++sk)
				sortKeys[t][sk] = sKeys.get(sk);
			
			sKeys.clear();
		
		}
		
	}
	
	
	/*Returns a list of all the attributes in a database*/
	public List<String> getAttributes(){
		return attributes;
	}

	
	public int[][] getJoinKeys(){
		return joinKeys;
	}
	
	public Table[]  getJoinOrder(){
		return joinOrder;
	}
	
	
	
	public Table[] getTables(){
		return tables;
	}
	
	public int[] getSortCols(int table){
		return sortKeys[table];
	}
	
	//Makes necessary computations for the second algorithm
	public void prepareDataForAlg2(){
		if(keysToAggregateOn == null)
			calcKeysAfter();
		else
			calcKeysAfterForSelectedAggs();
	}
	
	
	private void calcFirstAppearingKeys(){
		
		Set<Integer> notYetAppeared = new HashSet<Integer>(attributes.size());
		List<Integer> appearingKeys = new ArrayList<Integer>();
		
		for(int a = 0; a < attributes.size(); ++a) notYetAppeared.add(a);
		
		for(int t = 0; t < joinOrder.length; ++t){
			Table table = joinOrder[t];
			for(int key : table.getKeys()){
				if(notYetAppeared.contains(key)){ //We found a key that appears for the first time in the join order
					appearingKeys.add(key);
					notYetAppeared.remove(key);
				}
			}
			int[] keysToAdd = new int[appearingKeys.size()];
			int[] colsToAdd = new int[appearingKeys.size()];

			for(int k = 0; k < appearingKeys.size(); ++k){
				keysToAdd[k] = appearingKeys.get(k);
				colsToAdd[k] = table.keyToCol(keysToAdd[k]);
				
			}
			
			firstAppearingKeys[t] = keysToAdd;
			firstAppearingCols[t] = colsToAdd;
			
			appearingKeys.clear();
		}
	}
	/**calcKeysAfter()
	 * This function is used to pre-compute "directions" for each step (the join at each table)
	 * of the second algorithm so that the computeAggregates() function in the main loop
	 * does not need to "think" about which aggregates and variables go where
	 * 
	 * We pre-compute which intermediate aggregates must be processed, computed or
	 * magnified at each step of the join order. At each step of the join, the same aggregates, columns
	 * sums etc are calculated over and over again and hence it makes sense to pre-compute these
	 * "directions" instead of having to figure out which aggregates to compute every time
	 *  we call computeAggregates() in our main loop as it will save a lot of overhead since
	 *  computeAggregates() is the main bottleneck
	 * 
	 */
	private void calcKeysAfter(){
		
		Set<Integer> setKeysAfter = new HashSet<Integer>();
		
		for(int t = tables.length - 1; t >= 0; --t){
			
			//Calculate the aggregates that would need to be updated in the event of 
			//duplicate join key (one of our optimizations in second algorithm)
			//These are all the aggregate pairs that always after the current table in the 
			//join order i.e. we compute SUM(AB) where A and B only appear after table T in join order
			
			aggsLater[t]     = new int[(setKeysAfter.size())*(1+setKeysAfter.size())/2];
			
			int x = 0;
			for(int k1 : setKeysAfter)
				for(int k2: setKeysAfter){
					if(k2 < k1) continue; //If we do SUM(AB), we do not need SUM(BA)

					aggsLater[t][x++] = k1*numKeys + k2;

				}

			keysAfter[t] = new int[setKeysAfter.size()];

			Iterator<Integer> it = setKeysAfter.iterator();
			
			//Compute the keys exclusively appearing after the table in the join order
			for(int k = 0; k < setKeysAfter.size(); ++k)
				keysAfter[t][k] = it.next();
			
			
			/*Arrange for computation of aggregates like SUM(XA)
			 * Where X is a key in this table that does not appear earlier in the join order and 
			 * and   A is a key that always appears later in the join order
			 */
			
			mixedAggs[t]	    = new int[setKeysAfter.size() * firstAppearingKeys[t].length*2];
			writeMixedAggsIn[t] = new int[setKeysAfter.size() * firstAppearingKeys[t].length];
			
			int y = 0;
			for(int afterK: setKeysAfter){
				for(int hereK: firstAppearingKeys[t]){
					writeMixedAggsIn[t][y/2] = Math.min(afterK, hereK)*numKeys + Math.max(afterK, hereK); //Where to store aggregate
					mixedAggs[t][y++] = afterK;	
					mixedAggs[t][y++] = hereK;
				}
			}

			
			//Compute aggregates SUM(XY) such that X and Y are two keys that appear for the first time
			//in the join order at table t 
			sameTableAggs[t] 	= new int[firstAppearingKeys[t].length*(firstAppearingKeys[t].length+1)/2];
			sameTableAggCols[t] = new int[firstAppearingKeys[t].length*(firstAppearingKeys[t].length+1)];

			
			int z = 0;
			for(int k1 : firstAppearingKeys[t])
				for(int k2: firstAppearingKeys[t]){
					if(k2 < k1) continue; //If we do SUM(AB), we do not need SUM(BA)
					
					sameTableAggs[t][z/2] 		= k1*numKeys+k2; //Location in aggregate array the aggregate will be stored
					
					sameTableAggCols[t][z++]   = joinOrder[t].keyToCol(k1);
					sameTableAggCols[t][z++]   = joinOrder[t].keyToCol(k2);
					
					
				}
			
			
			//Add the keys that never appear earlier in the join order to the set of keys exclusively appearing after T-1
			for(int key : firstAppearingKeys[t])
				setKeysAfter.add(key);
			
		}
	
		
	}


	private boolean aggExists(int A, int B){
		for(int[] agg: keysToAggregateOn)
			if((agg[0] == A) && (agg[1] == B)) 
				return true;

		return false;

	}

	private void calcKeysAfterForSelectedAggs(){

		Set<Integer> setKeysAfter = new HashSet<Integer>();


		for(int t = tables.length - 1; t >= 0; --t){

			//Calculate the aggregates that would need to be updated in the event of 
			//duplicate join key (one of our optimizations in second algorithm)
			//These are all the aggregate pairs that always after the current table in the 
			//join order i.e. we compute SUM(AB) where A and B only appear after table T in join order
			
			ArrayList<Integer> aggsLaterList = new ArrayList<Integer>();

			for(int[] agg: keysToAggregateOn){
				if(setKeysAfter.contains(agg[0]) && setKeysAfter.contains(agg[1]))
					aggsLaterList.add(agg[0]*numKeys+agg[1]);
			}
			
			aggsLater[t] = new int[aggsLaterList.size()];
			
			
			for(int a = 0; a < aggsLater[t].length; ++a)
				aggsLater[t][a] = aggsLaterList.get(a);
			
			
			keysAfter[t] = new int[setKeysAfter.size()];

			Iterator<Integer> it = setKeysAfter.iterator();
			
			//Compute the keys exclusively appearing after the table in the join order
			for(int k = 0; k < setKeysAfter.size(); ++k){
				keysAfter[t][k] = it.next();
			}
			/*Arrange for computation of aggregates like SUM(XA)
			 * Where X is a key in this table that does not appear earlier in the join order and 
			 * and   A is a key that always appears later in the join order
			 */
			

			ArrayList<Integer> mixed 	  = new ArrayList<Integer>();
			ArrayList<Integer> writeMixed = new ArrayList<Integer>();


			for(int afterK: setKeysAfter){
				for(int hereK: firstAppearingKeys[t]){
					int min = Math.min(afterK, hereK); int max = Math.max(afterK, hereK);
					if(aggExists(min,max)){
						writeMixed.add( min*numKeys + max);
						mixed.add(afterK); 
						mixed.add(hereK); 

					}
				}
			}
			
			mixedAggs[t]	    = new int[mixed.size()];
			writeMixedAggsIn[t] = new int[writeMixed.size()];
			
			for(int a = 0; a < mixedAggs[t].length; ++a)
				mixedAggs[t][a] = mixed.get(a);
			
			for(int a = 0; a < writeMixedAggsIn[t].length; ++a)
				writeMixedAggsIn[t][a] = writeMixed.get(a);


			//Compute aggregates SUM(XY) such that X and Y are two keys that appear for the first time
			//in the join order at table t 
			
			ArrayList<Integer> sameAggs 	  = new ArrayList<Integer>();
			ArrayList<Integer> sameCols 	  = new ArrayList<Integer>();

			
			for(int k1 : firstAppearingKeys[t])
				for(int k2: firstAppearingKeys[t]){
					if(k2 < k1) continue; //If we do SUM(AB), we do not need SUM(BA)
					if(!aggExists(k1, k2))
						continue;
					
					sameAggs.add(k1*numKeys+k2);
					sameCols.add(joinOrder[t].keyToCol(k1)); sameCols.add(joinOrder[t].keyToCol(k2));
					
					
				}
			
			sameTableAggs[t]	    = new int[sameAggs.size()];
			sameTableAggCols[t]	    = new int[sameCols.size()];
			
			
			for(int a = 0; a < sameTableAggs[t].length; ++a)
				sameTableAggs[t][a] = sameAggs.get(a);
			
			for(int a = 0; a < sameCols.size(); ++a)
				sameTableAggCols[t][a] = sameCols.get(a);
			
			
			//Add the keys that never appear earlier in the join order to the set of keys exclusively appearing after T-1
			for(int key : firstAppearingKeys[t])
				setKeysAfter.add(key);
			
		}
	
		
	}
	
	
	public int[] getFirstAppearingCols(int table){
		return firstAppearingCols[table];
	}
	
	public int[] getFirstAppearingKeys(int table){
		return firstAppearingKeys[table];
	}
	
	public int[] getSameTableAggs(int table){
		return sameTableAggs[table];
	}
	
	public  int[] getSameTableCols(int table){
		return sameTableAggCols[table];
	}
	
	public int[] getMixedAggs(int table){
		return mixedAggs[table];
	}
	
	public int[] getWriteMixedAggs(int table){
		return writeMixedAggsIn[table];
	}
	
	public int[] getKeysAfter(int table){
		return keysAfter[table];
	}
	
	public int[] getLaterAggs(int table){
		return aggsLater[table];
	}
	
	public boolean[] getJoinKeysAfter(int table){
		return joinKeysAfter[table];
	}
	
	public void printJoinOrder(){
		for(Table table: joinOrder) System.out.println(table.name);
	}
	
	public int[] getJoinKeys(int table){
		return joinKeys[table];
	}
	
	public int[][] getKeysToAggregateOn(){
		return keysToAggregateOn;
	}
	
	
	
}

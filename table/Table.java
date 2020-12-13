package table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;

public class Table {

	protected final HashMap<String, Integer> attMap; 
	protected final double[][] data;
	private   final String[]   schema;
	public          int[]      sortedBy; //The columns the table is sorted on
	public    final String     name;
	private   static int       numTables = 0;
	private          int[]	   keys; //A user may assign a numeric schema to the table as well, where each attribute is mapped
    									 // to its corresponding ID in the database 
	
	public Table(double[][] table, String[] schema, String name) {
		this.data  = table; 
		attMap  = new HashMap<String, Integer>(schema.length * 2);
		this.schema = schema;
		this.name   = name != null? name : ("Table " + numTables++);
		
		//Create mapping from attribute name to column number 
		for(int att = 0; att < schema.length; ++att)
			attMap.put(schema[att], att); 
		
	}
	
	public Table(double[][] table, String[] schema) {
		this(table, schema, "Table " + numTables++);
	}
	
	
	

/**	Parses a table located with path file. Each row in the table is represented by a new line in the file
 *	where each attribute is separated by separator. The schema is given by the String array schema
 *
 *	The function returns a table object representing the relation.
 */
	public static Table getTable(String _file, String seperator, String[] schema, String name){
		
		int cols = schema.length;
		double[] row;
		ArrayList<double[]> rows = new ArrayList<double[]>();
		String[] strRow;
		
		BufferedReader reader = null;

		try {
		    File file = new File(_file);
		    reader = new BufferedReader(new FileReader(file));

		    String line;
		    while ((line = reader.readLine()) != null) {
		    	
		    	row = new double[cols];
		        strRow = line.split(seperator);
		        for(int c = 0; c < cols; ++c)
		        	row[c] = Long.parseLong(strRow[c]);
		       
		        rows.add(row);	       
		    }
		    
		   
		    
		return new Table(rows.toArray(new double[rows.size()][]), schema, name);

		} catch (IOException e) {
		    e.printStackTrace();
		} finally {
		    try {
		        reader.close();
		    } catch (IOException e) {
		        e.printStackTrace();
		    }
		}
		
	    return null;
	}
	
	public void sort(String[] strSortOn){
		int[] sortOn = new int[strSortOn.length];
		for(int at = 0; at < strSortOn.length; ++at)
			sortOn[at] = attMap.get(strSortOn[at]);
		
		Arrays.sort(data, new RowComparator(sortOn));
		sortedBy = sortOn;
		
	}
	
	public void sort(int[] sortOn){
		Arrays.sort(data, new RowComparator(sortOn));
		sortedBy = sortOn;
	}
	
	/**Sometimes we want to sort on say columns c1,c2,....cn but only consider the table sorted on c1,c2,...,cm where m <n
	 * In other words, this functions allows us to "pretend" the table was sorted on a smaller precision than it actually was
	 *     */
	public void sort(int[] sortOn, int[] colsToRemember){
		Arrays.sort(data, new RowComparator(sortOn));
		sortedBy = colsToRemember;
	}
	
	/**Finds the index of the first row with a matching key. If no such index exists, returns -1*/
	public int binarySearch(double[] key){
		
		if(sortedBy == null)
			return -1;

		int index = Arrays.binarySearch(data, key, new RowComparator(sortedBy));
		
		double[] row;
		boolean pass = true;
		
		
		//In case the binary search did not return the first value matching the index
		while(index > 0 && pass){
			
			row = data[index-1];
			
			for(int col : sortedBy)
				if(row[col] != key[col]){
					pass = false;
					break;
				}
			
			if(pass) index--; 

		}
		
		return index;
		
	}
	
	/**Finds the index of the first row with a matching key after row "after". If no such index exists, returns -1*/
	public int findAfter(double[] key, int after, int[] joinCols){
		
		int index = after + 1;
		boolean pass; double[] row;
		
		while(true){
			
			if(index == data.length) return -1;
			
			pass = true;
			row = data[index];
			
			for(int col : joinCols)
				if(row[col] != key[col]){
					pass = false;
					break;
				}
			
			if(pass) return index;
			
			++index;
		}
		
	}
	
	
	public void print(){
		for(double[] row : data){
			for(double val : row)
				System.out.print(val + "  ");
			System.out.println();
		}
	}
	
	public void intPrint(){
		for(double[] row : data){
			for(double val : row)
				System.out.print((int) val + "  ");
			System.out.println();
		}
	}
	
	public String[] getSchema(){
		return schema;
	}
	
	public String getAttribute(int idx){
		return schema[idx];
	}
	
	public int keyAtCol(int col){
		return keys[col];
	}
	
	
	public int getSize(){
		return data.length;
	}
	
	public int[] getKeys(){
		return keys;
	}
	
	public void setKeys(int[] _keys){
		this.keys = _keys;
	}
	
	public int numCols(){
		return schema.length;
	}
	
	public double[] getRow(int row){
		return data[row];
	}
	
	//Returns the column the key corresponds to if it exists
	public int keyToCol(int key){
		for(int c = 0; c < keys.length; ++c)
			if(keys[c] == key)
				return c;
		return -1;
	}
	
	public double valueAt(int col, int row){
		//System.out.println(col + "   " + row + "    " + data[0].length);
		return data[row][col];
	}
	

	
	
	
}

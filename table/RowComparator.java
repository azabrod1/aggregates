package table;

import java.util.Comparator;

/* A comparator class that defines how we compare rows. The inputs to this class is the list
 * of attributes which we will compare the rows on, in order of most to least important
 */
public class RowComparator implements Comparator<double[]>
{
	int[] compareOn;
	
	public RowComparator(int[] compareOn){
		this.compareOn = compareOn;
	}
	
	public void updateComparator(int[] newAtts){
		this.compareOn = newAtts;
	}
	
	
    public int compare(double[] row1, double[] row2)
    {
    	double difference;
    	for(int comp = 0; comp < compareOn.length; ++comp){
    		difference = row1[compareOn[comp]] - row2[compareOn[comp]];
    		if(difference != 0)
    			return (int) difference;
    		
    	}
    	return 0; 
    }
}
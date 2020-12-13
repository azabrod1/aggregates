package table;

public interface TableIterator {

	/**Returns true if the value of its join keys have changed*/
	public void synchronize();
	
	public boolean hasNext();
	
	public boolean increment();
	
	public int currentRow();
}
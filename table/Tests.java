package table;

/** Main file to run the algorithms and benchmark them*/


import algo1.JoinAlg;
import algo1.JoinAlgNaive;
import algo2.JoinAlg2;

public class Tests {

	public static final String seperator = "\\|";
	
	public static void main(String[] args) {
			
		
		/**Change the file path to run tests on a different machine. For example, if you want to run the algorithms for housing-5
		 * and the file's path is /Users/Downloads/Housing/housing-5 you set FILE_PATH = /Users/Downloads/Housing/
		 */
		
		final String FILE_PATH = "/Users/alex/Downloads/Housing/";
		
		Table [] tables = loadRelations(FILE_PATH, 8); //Change the number to use different housing dataset

		long start = System.currentTimeMillis(); 

		//JoinAlgNaive.run(tables); //UNCOMMENT TO RUN Naive algorithm for the specific dataset and have it print out results
		
		JoinAlg.run(tables);  	// UNCOMMENT TO RUN AggDB algorithm one for the specific datset above and print aggregates
		
		//JoinAlg2.run(tables);      //UNCOMMENT TO RUN AggDB algorithm two for the specific datset above and print aggregates
		
		
		/** If you want to run the algorithm for specific aggregates only rather than finding all aggregates, use the following code and change
		 * it to suit testing needs. We support using as many or as little aggregates as needed
		/** 
		 * String[][] keysToAggregate = {  {"tesco","kitchensize"},  {"postcode","postcode"}};
        	JoinAlg2.run(tables, keysToAggregate);	
        	*/

        System.out.println("Time taken: " + (System.currentTimeMillis() - start));
		
        
        /**We created a test to benchmark the three algorithms by running it on datasets 1 through 20.
         * For each data set, we run the test 5 times and report the average time of the last four runs.
         * Uncomment below code to run tests. 
         */
        
        				/*BENCHMARK CALCULATION OF ALL AGGREGATES*/
        
     //   benchmarkAlgoNaive(20, FILE_PATH); 	//UNCOMMENT TO BENCHMARK NAIVE ALGORITHM
        
     // benchmarkAlgo1(20, FILE_PATH);      //UNCOMMENT TO BENCHMARK ALGORITHM  1
        
     //   benchmarkAlgo2(20, FILE_PATH);	   	//UNCOMMENT TO BENCHMARK ALGORITHM  2
        
        
						/*BENCHMARK CALCULATION OF SELECT AGGREGATES*/

     
		String[][] keysToAggregate = {  {"tesco","kitchensize"}}; //add any aggregates you wish the algorithm to compute (supports more than 1)
		
		//benchmarkAlgoNaiveWithSelectAggs(20, FILE_PATH, keysToAggregate); //UNCOMMENT TO BENCHMARK NAIVE ALGORITHM WITH SELECT AGGS
		//benchmarkAlgo1WithSelectAggs(20, FILE_PATH, keysToAggregate);	  //UNCOMMENT TO BENCHMARK ALGORITHM 1 WITH SELECT AGGS
		//benchmarkAlgo2WithSelectAggs(20, FILE_PATH, keysToAggregate);	  //UNCOMMENT TO BENCHMARK ALGORITHM 2 WITH SELECT AGGS

		
		
	}
	
	/*Runs the algorithm on Housing Data set 1 through untilSetID and collects run times for each run
	 * Run times are calculated by running the algorithm on the data set 5 times, and averaging the last four runs
	 */
	
	private static final short NUM_RUNS = 5;
	
	public static long[] benchmarkAlgoNaive(int untilSetID, String FILE_PATH){
		
		long[] runTimes = new long[untilSetID];
		
		for(int dataSet = 1; dataSet <= untilSetID; ++dataSet){
			
			Table [] tables = loadRelations(FILE_PATH, dataSet);

	        JoinAlgNaive.runWithoutPrint(tables); //Discard first result
	        
	        long start = System.currentTimeMillis();
	        
	        for(int run = 1; run < NUM_RUNS; ++run)
	        	JoinAlgNaive.runWithoutPrint(tables);
	        	
	        
	        runTimes[dataSet-1] = (System.currentTimeMillis() - start)/(NUM_RUNS-1);
	        System.out.println("Average Time Taken for dataset " + dataSet + ": " + runTimes[dataSet-1]);
			
		}
	
        return runTimes;

	}
	
	//Same as above, use to test algorithm with first improvement over naive algorithm
	public static long[] benchmarkAlgo1(int untilSetID, String FILE_PATH){
		
		long[] runTimes = new long[untilSetID];
		
		for(int dataSet = 1; dataSet <= untilSetID; ++dataSet){
			
			Table [] tables = loadRelations(FILE_PATH, dataSet);

	        JoinAlg.runWithoutPrint(tables); //Discard first result
	        
	        long start = System.currentTimeMillis();
	        
	        for(int run = 1; run < NUM_RUNS; ++run)
	        	JoinAlg.runWithoutPrint(tables);
	        	
	        
	        runTimes[dataSet-1] = (System.currentTimeMillis() - start)/(NUM_RUNS-1);
	        System.out.println("Average Time Taken for dataset " + dataSet + ": " + runTimes[dataSet-1]);
			
		}
	
        return runTimes;

	}
	
	//Same as above, use to test algorithm with both improvements over naive algorithm
	public static long[] benchmarkAlgo2(int untilSetID, String FILE_PATH){
		
		long[] runTimes = new long[untilSetID];
		
		for(int dataSet = 1; dataSet <= untilSetID; ++dataSet){
			
			Table [] tables = loadRelations(FILE_PATH, dataSet);

	        JoinAlg2.runWithoutPrint(tables); //Discard first result
	        
	        long start = System.currentTimeMillis();
	        
	        for(int run = 1; run < NUM_RUNS; ++run)
	        	JoinAlg2.runWithoutPrint(tables);
	        	
	        
	        runTimes[dataSet-1] = (System.currentTimeMillis() - start)/(NUM_RUNS-1);
	        System.out.println("Average Time Taken for dataset " + dataSet + ": " + runTimes[dataSet-1]);
			
		}
	
        return runTimes;

	}
	
	//Use to benchmark NaiveDB with select aggregates
	public static long[] benchmarkAlgoNaiveWithSelectAggs(int untilSetID, String FILE_PATH, String[][] aggs){
		
		long[] runTimes = new long[untilSetID];
		
		for(int dataSet = 1; dataSet <= untilSetID; ++dataSet){
			
			Table [] tables = loadRelations(FILE_PATH, dataSet);

	        JoinAlgNaive.runWithoutPrint(tables, aggs); //Discard first result
	        
	        long start = System.currentTimeMillis();
	        
	        for(int run = 1; run < NUM_RUNS; ++run)
	        	JoinAlgNaive.runWithoutPrint(tables,aggs);
	        	
	        
	        runTimes[dataSet-1] = (System.currentTimeMillis() - start)/(NUM_RUNS-1);
	        System.out.println("Average Time Taken for dataset " + dataSet + ": " + runTimes[dataSet-1]);
			
		}
	
        return runTimes;

	}
	
	//Same as above, use to test algorithm with first improvement over naive algorithm with select aggregates only
	public static long[] benchmarkAlgo1WithSelectAggs(int untilSetID, String FILE_PATH, String[][] aggs){
		
		long[] runTimes = new long[untilSetID];
		
		for(int dataSet = 1; dataSet <= untilSetID; ++dataSet){
			
			Table [] tables = loadRelations(FILE_PATH, dataSet);

	        JoinAlg.runWithoutPrint(tables, aggs); //Discard first result
	        
	        long start = System.currentTimeMillis();
	        
	        for(int run = 1; run < NUM_RUNS; ++run)
	        	JoinAlg.runWithoutPrint(tables, aggs);
	        	
	        
	        runTimes[dataSet-1] = (System.currentTimeMillis() - start)/(NUM_RUNS-1);
	        System.out.println("Average Time Taken for dataset " + dataSet + ": " + runTimes[dataSet-1]);
			
		}
	
        return runTimes;

	}
	
	//Use to test algorithm 2 with selected aggregates
	public static long[] benchmarkAlgo2WithSelectAggs(int untilSetID, String FILE_PATH, String[][] aggs){
		
		long[] runTimes = new long[untilSetID];
		
		for(int dataSet = 1; dataSet <= untilSetID; ++dataSet){
			
			Table [] tables = loadRelations(FILE_PATH, dataSet);

	        JoinAlg2.runWithoutPrint(tables, aggs); //Discard first result
	        
	        long start = System.currentTimeMillis();
	        
	        for(int run = 1; run < NUM_RUNS; ++run)
	        	JoinAlg2.runWithoutPrint(tables, aggs);
	        	
	        
	        runTimes[dataSet-1] = (System.currentTimeMillis() - start)/(NUM_RUNS-1);
	        System.out.println("Average Time Taken for dataset " + dataSet + ": " + runTimes[dataSet-1]);
			
		}
	
        return runTimes;

	}
	
	
	
	/*Loads in the housing data tables*/
	public static Table[] loadRelations(String path, int setID){
		
		String[] names = {"House", "Shop", "Institution", "Restaurant", "Demographics", "Transport"};
		
		String[] schemas   = { 
				"postcode,livingarea,price,nbbedrooms,nbbathrooms,kitchensize,house,flat,condo,garden,parking",
				"postcode,openinghoursshop,pricerangeshop,sainsburys,tesco,ms",
				"postcode,typeeducation,sizeinstitution",
				"postcode,openinghoursrest,pricerangerest",
				"postcode,averagesalary,crimesperyear,unemployment,nbhospitals",
				"postcode,nbbuslines,nbtrainstations,distancecitycentre"	
			};
			
		
		Table[]  relations = new Table[names.length];

		
		for(int r = 0; r < relations.length; ++r)
			relations[r] = Table.getTable(path + "housing-" + setID + "/" + names[r] + ".tbl", seperator, schemas[r].split(","), names[r]);
		
		
		return relations;
	}
	
	
	
	
}

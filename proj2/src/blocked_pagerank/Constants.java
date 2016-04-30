package blocked_pagerank;

public class Constants {

	public static final String DELIMITER = "$";
	public static final String DELIMITER_REGEX = "\\$";
	public static final String PAGE_RANK = "PR";
	public static final String EDGES_FROM_BLOCK = "BE";
	public static final String BOUNDARY_CONDITION = "BC";
	public static final Double RESIDUAL_ERROR_THRESHOLD = 0.001;
	public static final Integer NUM_BLOCKS = 68;
	public static final Integer COUNTER_FACTOR = 100000000;
	public static final int MAX_ITERATIONS = 20;
	public static final Double DAMPING_FACTOR = 0.85;
	public static final int NUM_NODES = 685230;
}

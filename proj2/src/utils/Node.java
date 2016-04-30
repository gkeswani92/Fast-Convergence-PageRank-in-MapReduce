package utils;

public class Node {

	Long nodeId;
	String edges;
	Integer degree;
	Double pageRank;
	
	public Long getNodeId() {
		return nodeId;
	}
	
	public void setNodeId(Long nodeId) {
		this.nodeId = nodeId;
	}
	
	public String getEdges() {
		return edges;
	}
	
	public void setEdges(String edges) {
		this.edges = edges;
	}
	
	public Integer getDegree() {
		return degree;
	}
	public void setDegree(Integer degree) {
		this.degree = degree;
	}
	
	public Double getPageRank() {
		return pageRank;
	}
	
	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}
}

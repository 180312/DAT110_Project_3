package no.hvl.dat110.node.client.test;


import no.hvl.dat110.rpc.ChordNodeContainer;

public class Process1 {

	public static void main(String[] args) throws Exception {
		new ChordNodeContainer("process1", 600000, true);
	}

}
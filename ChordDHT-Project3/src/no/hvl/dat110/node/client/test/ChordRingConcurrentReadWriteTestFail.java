package no.hvl.dat110.node.client.test;


import java.math.BigInteger;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import no.hvl.dat110.rpc.ChordNodeContainer;
import no.hvl.dat110.rpc.StaticTracker;
import no.hvl.dat110.rpc.interfaces.ChordNodeInterface;
import no.hvl.dat110.util.Hash;

class ChordRingConcurrentReadWriteTestFail {
	
	private ChordNodeInterface p1;
	private ChordNodeInterface p2;
	private ChordNodeInterface p3;
	private ChordNodeInterface p4;
	private ChordNodeInterface p5;
	private ChordNodeInterface p6;
	private ChordNodeInterface p7;
	private ChordNodeInterface p8;
	private ChordNodeInterface p9;
	private ChordNodeInterface p10;

	@BeforeEach
	void setUp() throws Exception {
		
		String node1 = "process1";
		String node2 = "process2";
		String node3 = "process3";
		String node4 = "process4";
		String node5 = "process5";
		String node6 = "process6";
		String node7 = "process7";
		String node8 = "process8";
		String node9 = "process9";
		String node10 = "process10";
		
		// To test the correctness - we will manually manipulate the processes
		// Get the registry  - running on local machine's IP
		Registry registry = LocateRegistry.getRegistry(StaticTracker.PORT);
		
		BigInteger node1id = Hash.hashOf(node1);					// get the hash value of the node's name
		BigInteger node2id = Hash.hashOf(node2);					// get the hash value of the node's name
		BigInteger node3id = Hash.hashOf(node3);					// get the hash value of the node's name
		BigInteger node4id = Hash.hashOf(node4);					// get the hash value of the node's name
		BigInteger node5id = Hash.hashOf(node5);					// get the hash value of the node's name
		BigInteger node6id = Hash.hashOf(node6);					// get the hash value of the node's name
		BigInteger node7id = Hash.hashOf(node7);					// get the hash value of the node's name
		BigInteger node8id = Hash.hashOf(node8);					// get the hash value of the node's name
		BigInteger node9id = Hash.hashOf(node9);					// get the hash value of the node's name
		BigInteger node10id = Hash.hashOf(node10);					// get the hash value of the node's name
		
		p1 = (ChordNodeInterface) registry.lookup(node1id.toString());	// Look up the registry for each remote object
		p2 = (ChordNodeInterface) registry.lookup(node2id.toString());
		p3 = (ChordNodeInterface) registry.lookup(node3id.toString());
		p4 = (ChordNodeInterface) registry.lookup(node4id.toString());
		p5 = (ChordNodeInterface) registry.lookup(node5id.toString());
		p6 = (ChordNodeInterface) registry.lookup(node6id.toString());
		p7 = (ChordNodeInterface) registry.lookup(node7id.toString());
		p8 = (ChordNodeInterface) registry.lookup(node8id.toString());
		p9 = (ChordNodeInterface) registry.lookup(node9id.toString());
		p10 = (ChordNodeInterface) registry.lookup(node10id.toString());
	}

	@Test
	void test() throws RemoteException, InterruptedException {
		// test quorum-based consistency protocol
		// concurrency test when 2 clients request to read and write to the same file at the same time
		// Here we are testing for concurrency access on the same file. Testing on different files may pass because
		// the there could be enough votes (replicas) for different items

		NodeClientReader r = new NodeClientReader("process10");						// send read request for file = process5 in the ring
		NodeClientWriter w = new NodeClientWriter("dat110-course", "process10"); 	// send write request to write into file = process5 in the ring
		
		r.start();
		w.start();
		
		r.join();
		w.join();
		
		boolean result = r.isSucceed() && w.isSucceed();
		
		Assertions.assertFalse(result); 										// test must fail as this should return false

	}

}

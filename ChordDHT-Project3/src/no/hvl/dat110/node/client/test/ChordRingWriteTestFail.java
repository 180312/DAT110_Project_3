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

class ChordRingWriteTestFail {
	
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
		// acquire read/write locks for 9 processes (N=10) we need N/2 + 1 to get permission
		// Here we don't know the number of replicas for a particular file. The minimum is 1
		// with 1 replica to a file, we need 1 replica to give permission
		p1.acquireLock();
		p2.acquireLock();
		p3.acquireLock();
		p4.acquireLock();
		p5.acquireLock();
		p6.acquireLock();
		p7.acquireLock();
		p8.acquireLock();
		p9.acquireLock();
		
		NodeClientWriter w = new NodeClientWriter("12345test", "process4");	// request to write 12345test into the file named process4
		w.start();
		w.join();
		
		p1.releaseLocks();
		p2.releaseLocks();
		p3.releaseLocks();
		p4.releaseLocks();
		p5.releaseLocks();
		p6.releaseLocks();
		p7.releaseLocks();
		p8.releaseLocks();
		p6.releaseLocks();
		Assertions.assertFalse(w.isSucceed()); 									// test must fail as this should return false (should not get the majority vote)
		
	}

}

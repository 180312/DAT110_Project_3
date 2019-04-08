package no.hvl.dat110.node;



/**
 * project/exercise/demo purpose in dat110
 * @author tdoy
 *
 */

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
//import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import no.hvl.dat110.rpc.StaticTracker;
import no.hvl.dat110.rpc.interfaces.ChordNodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;

public class Node extends UnicastRemoteObject implements ChordNodeInterface {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private BigInteger nodeID;		// BigInteger value of hash of IP address of the Node
	private String nodeIP;			// IP address of node 
	private ChordNodeInterface successor;
	private ChordNodeInterface predecessor;
	private List<ChordNodeInterface> fingerTable;
	private Set<BigInteger> fileKey;
	private Map<BigInteger, Message> filesMetadata;
	
	// variables for mutex and quorum-based protocols
	private List<Message> queue;							// queue for this process
	private List<Message> queueACK; 						// queue for acknowledged messages
	private Set<Message> activenodesforfile;

	private int counter;									// Lamport local clock
	private boolean CS_BUSY = false;						// indicate to be in critical section (accessing a shared resource) 
	private boolean WANTS_TO_ENTER_CS = false;				// indicate to want to enter CS
	private int quorum;
	
	public Node(String nodename) throws RemoteException, UnknownHostException {
		super();

		fingerTable = new ArrayList<ChordNodeInterface>();
		fileKey = new HashSet<BigInteger>();
		// setNodeIP(InetAddress.getLocalHost().getHostAddress());	// use the IP address of the host
		setNodeIP(nodename);										// use a different name as "IP" for single machine simulation
		BigInteger hashvalue = Hash.hashOf(getNodeIP());			// use the SHA-1  from Hash class
		setNodeID(hashvalue);
		
		setSuccessor(null);
		setPredecessor(null);

		filesMetadata = new HashMap<BigInteger, Message>();
		
		counter = 0;
		queue = new ArrayList<Message>();	
		queueACK = new ArrayList<Message>();
		queueACK = Collections.synchronizedList(queueACK);			// make sure to sybchronize list
	}
	
	public BigInteger getNodeID() {
		return nodeID;
	}
	
	public void setNodeID(BigInteger nodeID) {
		this.nodeID = nodeID;
	}
	
	public String getNodeIP() {
		return nodeIP;
	}

	public void setNodeIP(String nodeIP) {
		this.nodeIP = nodeIP;
	}

	public ChordNodeInterface getSuccessor() {
		return successor;
	}
	
	public void setSuccessor(ChordNodeInterface successor) {
		this.successor = successor;
	}
	
	public ChordNodeInterface getPredecessor() {
		return predecessor;
	}
	public void setPredecessor(ChordNodeInterface predecessor) {
		this.predecessor = predecessor;
	}
	
	public List<ChordNodeInterface> getFingerTable() {
		return fingerTable;
	}
	
	public void addToFingerTable(ChordNodeInterface finger) {
		this.fingerTable.add(finger);
	}
	
	public void removeFromFingerTable(ChordNodeInterface finger) {
		this.fingerTable.remove(finger);
	}
	
	public void setFingerTable(List<ChordNodeInterface> fingerTable) {
		this.fingerTable = fingerTable;
	}

	public Set<BigInteger> getFileKey() {
		return fileKey;
	}
	
	public void addToFileKey(BigInteger fileKey) {
		this.fileKey.add(fileKey);
	}
	
	public void removeFromFileKey(BigInteger fileKey) {
		this.fileKey.remove(fileKey);
	}

	@Override
	public ChordNodeInterface findSuccessor(BigInteger keyid) throws RemoteException {
		
		// ask this node to find the successor of id
		ChordNodeInterface succ = this.getSuccessor();			// last known successor of this node
			
		ChordNodeInterface succstub = Util.registryHandle(succ); 	// issue a remote call and see if this node is still active
			
		if(succstub != null) {
			
			//System.out.println("Successor for Node: successor("+this.getNodeIP()+") = "+succstub.getNodeIP());
			
			BigInteger succID = succstub.getNodeID();
			BigInteger nodeID = this.getNodeID();

			// check that keyid is a member of the set {nodeid+1,...,succID}
			Boolean cond = Util.computeLogic(keyid, nodeID.add(new BigInteger("1")), succID);
	
			if(cond) {
				return succstub;
			} else {
				// search the local finger table of this node for the highest predecessor of id
				ChordNodeInterface highest_pred = findHighestPredecessor(keyid);
				return highest_pred.findSuccessor(keyid);							// a remote call
			}
		}
				
		return null;	
	}
	
	private ChordNodeInterface findHighestPredecessor(BigInteger ID) throws RemoteException {
		
		BigInteger nodeID = getNodeID();
		List<ChordNodeInterface> fingers = getFingerTable();			
		
		int size = fingers.size() - 1;
		//System.out.println("FingerTable size: "+fingers.size());
		for(int i=0; i<fingers.size()-1; i++) {
			int m = size-i;
			ChordNodeInterface ftsucc = fingers.get(m);
			try {
				BigInteger ftsuccID = ftsucc.getNodeID();
				
				Registry registry = Util.locateRegistry(ftsucc.getNodeIP());
				
				if(registry == null)
					return this;

				ChordNodeInterface ftsuccnode = (ChordNodeInterface) registry.lookup(ftsuccID.toString());

				// check that ftsuccID is a member of the set {nodeID+1,...,ID-1}
				boolean cond = Util.computeLogic(ftsuccID, nodeID.add(new BigInteger("1")), ID.subtract(new BigInteger("1")));
				if(cond) {
					return ftsuccnode;
				}
			} catch (Exception e) {
				//e.printStackTrace();
			}			
		}
		
		return (ChordNodeInterface) this;			
	}
	
	@Override
	public void notifySuccessor(ChordNodeInterface pred_new) throws RemoteException {
		
		ChordNodeInterface pred_old = this.getPredecessor();
		
		if(pred_old == null) {
			this.setPredecessor(pred_new);		// accept the new predecessor
			return;
		}
		
		BigInteger succID = this.getNodeID();
		BigInteger pred_oldID = pred_old.getNodeID();
		
		BigInteger pred_newID = pred_new.getNodeID();
		
		// check that ftsuccID is a member of the set {nodeID+1,...,ID-1}
		boolean cond = Util.computeLogic(pred_newID, pred_oldID.add(new BigInteger("1")), succID.add(new BigInteger("1")));
		if(cond) {		
			this.setPredecessor(pred_new);		// accept the new predecessor
		}		
		
	}
	
	@Override
	public void createFileInNodeLocalDirectory(String initialcontent, BigInteger destID) throws RemoteException {
		String path = new File(".").getAbsolutePath().replace(".", "");
		//System.out.println(path);
		String destpath = path+"/"+this.getNodeIP()+"/"+destID;
		
		File file = new File(destpath);
		
		try {
			if(!file.exists())					// if replica already exist don't destroy it
				file.createNewFile();
		} catch (IOException e) {
			//e.printStackTrace();
		}
		
		// write initial content in the file - i.e. the local node's info
		Util.writetofile(initialcontent, file);
		
		// wrap the file details and node details in a message (metadata of file)	
		buildMessage(destID, destpath);
	}
	
	public Map<BigInteger, Message> getFilesMetadata() throws RemoteException {
		return filesMetadata;
	}

	private void buildMessage(BigInteger destID, String destpath) throws RemoteException {
		
		Message message = new Message();
		message.setNodeID(getNodeID());
		message.setNodeIP(getNodeIP());
		message.setFilename(destID);
		message.setFilepath(destpath);									// absolute path to the resource (file)
		message.setVersion(0); 											// This is a new replica (version=0) to be managed by this node
		filesMetadata.put(destID, message);								// save the replica and its metadata
	}
		
	/*
	 *  Section: Mutual exclusion and quorum-based protocol implementation
	 * 
	 */
	
	@Override
	public void incrementclock() throws RemoteException {
		counter++;
	}
	
	@Override
	public void acquireLock() throws RemoteException {
		incrementclock();
		CS_BUSY = true;
	}
	
	@Override
	public void releaseLocks() throws RemoteException {
		CS_BUSY = false;
		WANTS_TO_ENTER_CS = false;
	}
	
	@Override
	public boolean requestWriteOperation(Message message) throws RemoteException {
		
		incrementclock();								// increment clock 
		message.setClock(counter);						// set the timestamp of message
		message.setOptype(OperationType.WRITE);

		// wants to access resource - multicast clock + message to other processes
		WANTS_TO_ENTER_CS = true;		
		
		// request for write permission from N/2 + 1 replicas (majority)
		
		return multicastMessage(message);
		
	}

	@Override
	public boolean requestReadOperation(Message message) throws RemoteException {
		
		incrementclock();								// increment clock 
		message.setClock(counter);						// set the timestamp of message
		message.setOptype(OperationType.READ);

		// wants to access resource - multicast clock + message to other processes
		WANTS_TO_ENTER_CS = true;		
		
		// request for read permission from N/2 + 1 replicas (majority)

		return multicastMessage(message);	
	}	
	
	// multicast message to N/2 + 1 processes (random processes)
	private boolean multicastMessage(Message message) throws AccessException, RemoteException {
		
		// the same as MutexProcess - see MutexProcess
		quorum = activenodesforfile.size()/2 + 1;
		List<Message> activenodes = new ArrayList<Message>(this.activenodesforfile);
		
		synchronized(queueACK) {
			queueACK.clear();
			for(int i=0; i<activenodes.size(); i++) {
				String stub = activenodes.get(i).getNodeID().toString();
				try {
					Registry reg = Util.locateRegistry(activenodes.get(i).getNodeIP());
					ChordNodeInterface node = (ChordNodeInterface) reg.lookup(stub);
					queueACK.add(node.onMessageReceived(message));
				} catch (NotBoundException e) {
					e.printStackTrace();
				}
				
			}
		}
		return majorityAcknowledged();	
	}
	
	@Override
	public Message onMessageReceived(Message message) throws RemoteException {
		
		// increment the local clock
		incrementclock();

		// Hint: for all 3 cases, use Message to send GRANT or DENY. e.g. message.setAcknowledgement(true) = GRANT
		
		/**
		 *  case 1: Receiver is not accessing shared resource and does not want to: GRANT, acquirelock and reply
		 */
		if(!CS_BUSY) {
			acquireLock();
			message.setAcknowledged(true);
			return message;
		}
		
		/**
		 *  case 2: Receiver already has access to the resource: DENY and reply
		 */
		
		if(CS_BUSY) {
			message.setAcknowledged(false);
			return message;
		}
		
		/**
		 *  case 3: Receiver wants to access resource but is yet to (compare own multicast message to received message
		 *  the message with lower timestamp wins) - GRANT if received is lower, acquirelock and reply
		 */	
		
		if(WANTS_TO_ENTER_CS) {
			if(message.getClock()<counter) {
				acquireLock();
				message.setAcknowledged(true);
				return message;
			} else {
				message.setAcknowledged(false);
				return message;
			}
		}
		
		
		return message;
		
	}
	
	@Override
	public boolean majorityAcknowledged() throws RemoteException {
		
		// count the number of yes (i.e. where message.isAcknowledged = true)
		long count = queueACK.stream().filter(message -> message.isAcknowledged()).count();
		
		quorum = activenodesforfile.size()/2 + 1;
		// check if it is the majority or not
		return count >= quorum;
	}

	@Override
	public void setActiveNodesForFile(Set<Message> messages) throws RemoteException {
		
		activenodesforfile = messages;
		
	}

	@Override
	public void onReceivedVotersDecision(Message message) throws RemoteException {
		
		// release CS lock if voter initiator says he was denied access bcos he lacks majority votes
		if(message.isAcknowledged()) {
			WANTS_TO_ENTER_CS = false;
		}
		// otherwise lock is kept

	}

	@Override
	public void onReceivedUpdateOperation(Message message) throws RemoteException {
		
		// check the operation type: we expect a WRITE operation to do this. 
		OperationType optype = message.getOptype();
		
		switch(optype) {
		case WRITE: {
			Operations op = new Operations(this, message, activenodesforfile);
			op.performOperation();
			releaseLocks();
		}
		break;

		default: break;
	}
		// perform operation by using the Operations class 
		// Release locks after this operation
		
	}
	
	@Override
	public void multicastUpdateOrReadReleaseLockOperation(Message message) throws RemoteException {
		
		// check the operation type:
		OperationType optype = message.getOptype();
		
		switch(optype) {
		case WRITE: {
			Operations op = new Operations(this, message, activenodesforfile);
			op.multicastOperationToReplicas(message);
		}
		break;
		case READ: {
			Operations op = new Operations(this, message, activenodesforfile);
			op.multicastReadReleaseLocks();
		}
		break;
		default: break;
		}
		// if this is a write operation, multicast the update to the rest of the replicas (voters)
		// otherwise if this is a READ operation multicast releaselocks to the replicas (voters)
	}	
	
	@Override
	public void multicastVotersDecision(Message message) throws RemoteException {	
		
		// multicast voters decision to the rest of the replicas (i.e activenodesforfile)
		List<Message> activenodes = new ArrayList<Message>(this.activenodesforfile);
		
		for(int i=0; i<activenodes.size(); i++) {
			String stub = activenodes.get(i).getNodeID().toString();
			try {
				Registry reg = Util.locateRegistry(activenodes.get(i).getNodeIP());
				ChordNodeInterface node = (ChordNodeInterface) reg.lookup(stub);
				node.onReceivedVotersDecision(message);
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
			
		}

	}

}

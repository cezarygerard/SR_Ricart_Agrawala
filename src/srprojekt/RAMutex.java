package srprojekt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class RAMutex implements Runnable {
	// TODO zrob z tego singleton z synchronizowanymi metodami

	private LinkedList<Node> nodes;
	private Node thisNode;
	private volatile int sequenceNumber;
	// private volatile boolean isBusy;
	// private int initHostPort;
	// private String initHostAddress;
	private ServerSocket serverSocket;
	private JSONParser parser;

	private static final int BAD_VALUE = -1;

	public RAMutex(HashMap<String, String> params) throws IOException {
		int initHostPort;
		String initHostAddress;
		parser = new JSONParser();

		sequenceNumber = 0;
		nodes = new LinkedList<Node>();
		thisNode = new Node();
		thisNode.setName(params.get("name"));

		thisNode.setAddress(params.get("address"));
		if (thisNode.getAddress() == null)
			thisNode.setAddress("127.0.0.1");

		int port = BAD_VALUE;
		String portStr = params.get("port");
		if (portStr != null) {
			port = Integer.parseInt(portStr);
		}
		thisNode.setPort(port);

		initHostAddress = params.get("init_host_addr");
		initHostPort = BAD_VALUE;

		String initHostPortStr = params.get("init_host_port");
		if (initHostPortStr != null) {
			initHostPort = Integer.parseInt(initHostPortStr);
		}

		if (initHostPort == BAD_VALUE) {
			// inicjuj siebie na sponsora
			if (port == BAD_VALUE)
				throw new InvalidParameterException(
						"both port and init_host_port not specified!");

			serverSocket = new ServerSocket(port);
		} else {// odpytuj sponsora
			serverSocket = new ServerSocket(0);
			thisNode.setPort(serverSocket.getLocalPort());
			doInit();
		}

		new Thread(this).start();

	}

	@Override
	public synchronized void run() {
		Socket clientSocket = null;
		try {
			clientSocket = serverSocket.accept();

			BufferedReader in = new BufferedReader(new InputStreamReader(
					clientSocket.getInputStream()));

			String inputLine = in.readLine();
			handleInput(inputLine);
			in.close();
			clientSocket.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private synchronized void handleInput(String inputLine) {
		// parser
		Object obj = null;
		try {
			obj = parser.parse(inputLine);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		JSONArray array=(JSONArray)obj;
		JSONObject obj2 = (JSONObject) array.get(1);
		
		System.out.println(obj2.get("TYPE"));
		
		int type = MsgType.handlerMap.get(obj2.get("TYPE"));
		switch (type) {
		case 1:
			
			break;

		default:
			System.out.println(obj2.get("Protocor error"));
		}
	}

	public synchronized void requestToken() {

	}

	public synchronized void releaseToken() {

	}

	private synchronized JSONObject prepareHeader(String msgType) {

		JSONObject header = new JSONObject();
		JSONObject headerObj = new JSONObject();
		headerObj.put("IP", thisNode.getAddress());
		headerObj.put("Port", thisNode.getPort());
		headerObj.put("UniqueName", thisNode.getName());
		header.put("FROM", headerObj);
		return header;
	}

	private synchronized void sendStuff(JSONObject stuff, LinkedList<Node> nodes) {

	}

	@Override
	protected void finalize() throws Throwable {
		try {
			// close sockets
		} finally {
			super.finalize();
		}
	}

	// private Inet4Address ipAddress;

	// private class Node
	private static class MsgType {
		public String INIT = "INIT";
		// public String RE_INIT = "RE_INIT" ;
		public static String REMOVE = "REMOVE";
		public static String REQUEST = "REQUEST";
		public static String REPLY = "REPLY";
		public static String ARE_YOU_THERE = "ARE_YOU_THERE";
		public static String YES_I_AM_HERE = "YES_I_AM_HERE";
		public static String HIGHEST_SEQ_NUM = "HIGHEST_SEQ_NUM";
		public static String DEAD = "DEAD";
	    private static final Map<String, Integer> handlerMap;
	   
	    static {
	        Map<String, Integer> aMap = new HashMap<String, Integer>();
	        aMap.put(REMOVE,1);
	        aMap.put(REQUEST, 2);
	        aMap.put(REMOVE, 3);
	        aMap.put(REPLY, 4);
	        aMap.put(ARE_YOU_THERE, 5);
	        aMap.put(YES_I_AM_HERE, 6);
	        aMap.put(HIGHEST_SEQ_NUM, 7);
	        aMap.put(DEAD,87);
	        handlerMap = Collections.unmodifiableMap(aMap);
	    }
	}

}

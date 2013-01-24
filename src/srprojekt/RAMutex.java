package srprojekt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class RAMutex implements Runnable {
	// TODO zrob z tego singleton z synchronizowanymi metodami

	private HashMap<String, Node> nodes;
	private Node thisNode;
	private volatile int sequenceNumber;
	private ServerSocket serverSocket;
	private JSONParser parser;
	private volatile boolean initDone;

	private static final int BAD_VALUE = -1;

	public RAMutex(HashMap<String, String> params) throws IOException {
		initDone = false;
		int initHostPort;
		String initHostAddress;
		parser = new JSONParser();

		sequenceNumber = 0;
		nodes = new HashMap<String, Node>();
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
			doInit(initHostAddress, initHostPort);
		}

		new Thread(this).start();

	}

	private synchronized void doInit(String initHostAddress, int initHostPort) {
		JSONObject jObject = new JSONObject();
		JSONObject jHeader = prepareHeader();
		JSONObject jRole = new JSONObject();
		jRole.put("Role", "New");

		jObject.put("CONTENT", jRole);
		jObject.put("FROM", jHeader);
		jObject.put("TYPE", "INIT");
		send(initHostAddress, initHostPort, jObject.toString());
	}

	@Override
	public void run() {
		Socket clientSocket = null;
		while (true) {
			try {
				clientSocket = serverSocket.accept();

				BufferedReader in = new BufferedReader(new InputStreamReader(
						clientSocket.getInputStream()));

				final String inputLine = in.readLine();

				new Thread(new Runnable() {
					public void run() {
						System.out.println("received: " + inputLine);
						handleInput(inputLine);
					}
				}).start();

				in.close();
				clientSocket.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private synchronized void handleInput(String inputLine) {
		// parser
		JSONObject jobj = null;
		try {
			jobj = (JSONObject) parser.parse(inputLine);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		// System.out.println(obj);

		// System.out.println(obj.get("TYPE"));
		String key = jobj.get("TYPE").toString();
		int type = MsgType.handlerMap.get(key.toLowerCase());
		switch (type) {
		case 0:
			handleInit(jobj); // ok
			break;
		case 1:
			// handleRemove(obj);
			break;
		case 2:
			handleRequest(jobj);
			break;

		case 3:
			// handleRemove(jobj);
			break;

		case 4:
			handleReply(jobj);
			break;

		case 5:
			handleAreYouThere(jobj); // ok
			break;

		case 6:

			break;

		case 7:
			handleHighestSeqNum(jobj);
			break;

		case 8:
			handleDead(jobj);
			break;

		default:
			System.out.println("Protocor error");
		}
	}

	private synchronized void handleDead(JSONObject jobj) {
		String status = (String) ((JSONObject) jobj.get("CONTENT"))
				.get("STATUS");
		if (status.equals("REMOVE")) {
			nodes.remove( (String) ((JSONObject) jobj.get("FROM")).get("UniqueName"));			
		} 
//		else if (status.equals("GET")) {
//
//		} 
		else
			System.out.println("Protocor error");

	}

	private synchronized void handleHighestSeqNum(JSONObject jobj) {
		System.out.println("handleHighestSeqNum not imlemented");

	}

	private synchronized void handleAreYouThere(JSONObject jobj) {

		JSONObject jObject = new JSONObject();
		JSONObject jHeader = prepareHeader();
		JSONObject jContent = new JSONObject();

		jObject.put("CONTENT", jContent);
		jObject.put("FROM", jHeader);
		jObject.put("TYPE", "YES_I_AM_THERE");
		String key = (String) ((JSONObject) jobj.get("FROM")).get("UniqueName");
		sendStuff(jObject, nodes.get(key));
	}

	private synchronized void handleReply(JSONObject jobj) {
		System.out.println("handleReply not imlemented");

	}

	private synchronized void handleRequest(JSONObject obj) {

		// daj dzialac innym
		JSONObject jObject = new JSONObject();
		JSONObject jHeader = prepareHeader();
		JSONObject jContent = new JSONObject();

		jObject.put("CONTENT", jContent);
		jObject.put("FROM", jHeader);
		jObject.put("TYPE", "REPLY");
		String key = (String) ((JSONObject) obj.get("FROM")).get("UniqueName");
		sendStuff(jObject, nodes.get(key));
		// TODO logika mutex'a

	}

	private synchronized void handleInit(JSONObject obj) {
		String role = (String) ((JSONObject) obj.get("CONTENT")).get("Role");
		if (role.equals("Sponsor")) {
			Node sponsor = new Node((JSONObject) obj.get("FROM"));
			nodes.put(sponsor.getName(), sponsor);
			JSONObject jnodes = (JSONObject) ((JSONObject) obj.get("CONTENT"))
					.get("NodesData");

			Set keys = jnodes.keySet();
			Iterator iterator = keys.iterator();
			while (iterator.hasNext()) {
				String key = (String) iterator.next();
				JSONObject jobj = (JSONObject) jnodes.get(key);
				nodes.put(key, (new Node(jobj, key)));
			}
			JSONObject jObject = new JSONObject();
			JSONObject jHeader = prepareHeader();
			JSONObject jcontent = new JSONObject();
			jcontent.put("Role", "Node");
			jcontent.put("NewData", jHeader);
			jObject.put("CONTENT", jcontent);
			jObject.put("FROM", jHeader);
			jObject.put("TYPE", "INIT");

			sendToAll(jObject);
		} else if (role.equals("Node")) {
			// ktos nowy sie pojawil
			JSONObject jnewNode = (JSONObject) ((JSONObject) obj.get("CONTENT"))
					.get("NewData");
			Node newOne = new Node(jnewNode);
			nodes.put(newOne.getName(), newOne);

		} else if (role.equals("New")) {
			JSONObject jObject = new JSONObject();
			JSONObject jHeader = prepareHeader();
			JSONObject jcontent = new JSONObject();
			jcontent.put("Role", "Sponsor");
			JSONObject jnodes = new JSONObject();
			// jnodes.
			for (Node n : nodes.values()) {
				jnodes.put(n.getName(), n.toJson());
			}
			
			Node jnewNode = new Node((JSONObject) obj.get("FROM"));
			nodes.put(jnewNode.getName(), jnewNode);
			jcontent.put("NodesData", jnodes);
			jObject.put("CONTENT", jcontent);
			jObject.put("FROM", jHeader);
			jObject.put("TYPE", "INIT");
			
			sendStuff(jObject, jnewNode);

		} else
			System.out.println("Protocor error");

	}



	private synchronized JSONObject prepareHeader() {
		JSONObject jHeaderObj = new JSONObject();
		jHeaderObj.put("Ip", thisNode.getAddress());
		jHeaderObj.put("Port", thisNode.getPort());
		jHeaderObj.put("UniqueName", thisNode.getName());
		return jHeaderObj;
	}

	private synchronized void sendToAll(JSONObject stuff) {
		Collection<Node> c = nodes.values();
		for (Node node : c) {
			send(node.getAddress(), node.getPort(), stuff.toString());
		}
	}

	private synchronized void sendStuff(JSONObject stuff, Node node) {
		send(node.getAddress(), node.getPort(), stuff.toString());

	}

	private synchronized void send(String initHostAddress, int port,
			String stuff) {
		try {
			Socket socket = new Socket(InetAddress.getByName(initHostAddress),
					port);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			out.println(stuff);
			out.close();
			socket.close();

			System.out.println("sending: " + stuff);
			// System.out.println("to: " + initHostAddress + "  " + port);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			doDie();
		} finally {
			super.finalize();
		}
	}

	private void doDie() {
		System.out.println("doDie not imlemented");
		
	}
	
	public synchronized void requestToken() {
		System.out.println("requestToken not imlemented");
	}

	public synchronized void releaseToken() {
		System.out.println("releaseToken not imlemented");
	}
	

	private static class MsgType {
		public static String INIT = "INIT";
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
			aMap.put(INIT.toLowerCase(), 0);
			// aMap.put(REMOVE.toLowerCase(), 1);
			aMap.put(REQUEST.toLowerCase(), 2);
			aMap.put(REMOVE.toLowerCase(), 3);
			aMap.put(REPLY.toLowerCase(), 4);
			aMap.put(ARE_YOU_THERE.toLowerCase(), 5);
			aMap.put(YES_I_AM_HERE.toLowerCase(), 6);
			aMap.put(HIGHEST_SEQ_NUM.toLowerCase(), 7);
			aMap.put(DEAD.toLowerCase(), 8);
			handlerMap = Collections.unmodifiableMap(aMap);
		}
	}
}

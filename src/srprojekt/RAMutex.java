package srprojekt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.json.simple.JSONObject;
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
	private volatile int initCount;
	private volatile boolean needsToken;
	private volatile boolean hasToken;
	private volatile int repliesCount;
	private Map<String, LinkedList<TimerTask>> timerLists;
	
	private Map<String, LinkedList<Runnable>> tasks;
	private Timer timer;
	private static final int BAD_VALUE = -1;
	private JSONObject jHeaderObj;
	private boolean verbose;
	private Object timerLockObj;
	private int maxUseTime;
	
	public RAMutex(HashMap<String, String> params) throws IOException {
		initDone = false;
		needsToken = false;
		hasToken = false;
		repliesCount = 0;
		initCount = 1;
		timer = new Timer();
		timerLockObj = new Object();
	//	timer = new Tim
		int initHostPort;
		String initHostAddress;
		parser = new JSONParser();
		// ConcurrentHashMap
		timerLists = new ConcurrentHashMap<String, LinkedList<TimerTask>>();
		tasks = new ConcurrentHashMap<String, LinkedList<Runnable>>();
		sequenceNumber = 0;
		nodes = new HashMap<String, Node>();
		thisNode = new Node();
		thisNode.setName(params.get("name"));
		maxUseTime = 10*1000;
		String maxUseTimeStr = params.get("maxusetime");
		if (maxUseTimeStr != null) {
			maxUseTime = 1000* Integer.parseInt(maxUseTimeStr);
		}
		
		String verboseStr = params.get("verbose");
		if (verboseStr != null) {
			verbose = Boolean.parseBoolean(verboseStr);
		}

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
			String a = serverSocket.getLocalSocketAddress().toString();
			String b = serverSocket.getLocalSocketAddress().toString();
			initDone = true;

		} else {// odpytuj sponsora
			serverSocket = new ServerSocket(0);
			String a = serverSocket.getLocalSocketAddress().toString();
			String b = serverSocket.getLocalSocketAddress().toString();
			thisNode.setPort(serverSocket.getLocalPort());
			doInit(initHostAddress, initHostPort);
		}

		new Thread(this).start();

		// a tu mozna przetestowac umieranie
		// new Thread(new Runnable() {
		// public void run() {
		// try {
		// Thread.sleep(1000*30);
		// doDie();
		// System.exit(0);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		//
		// }
		// }).start();

	}

	private void dispatchInput(String inputLine) {
		// parser
		inputLine = inputLine.toUpperCase();
		JSONObject jobj;
		int type = BAD_VALUE;
		synchronized (this) {
			jobj = null;
			try {
				jobj = (JSONObject) parser.parse(inputLine);
			} catch (ParseException e) {
				// e.printStackTrace();
			}
		}
		String key = jobj.get("TYPE").toString();
		if (verbose)
			System.out.println("dispatcher key: " + key);
		try {
			type = MsgType.handlerMap.get(key.toUpperCase());
		} catch (NullPointerException e) {
			// e.printStackTrace();
		}

		cancelTimer(new Node((JSONObject) jobj.get("FROM")));

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
			System.out.println("Protocor error: " + jobj);
		}
	}

	private synchronized void handleDead(JSONObject jobj) {
		String status = (String) ((JSONObject) jobj.get("CONTENT"))
				.get("STATUS");
		if (status.equals("REMOVE")) {
			nodes.remove((String) ((JSONObject) jobj.get("FROM"))
					.get("UNIQUENAME"));
		}
		// else if (status.equals("GET")) {
		//
		// }
		else
			System.out.println("Protocor error: " + jobj);

	}

	private void handleHighestSeqNum(JSONObject jobj) {
		String status = (String) ((JSONObject) jobj.get("CONTENT"))
				.get("STATUS");
		if (status.equals("GET")) {
			// /czy powinienem wysylac, jak dopiero przyszedlem do sieci??
			// if(!this.initDone)??

			JSONObject jObject = new JSONObject();
			JSONObject jHeader = prepareHeader();
			JSONObject jContent = new JSONObject();
			jContent.put("VALUE", this.sequenceNumber);
			jContent.put("STATUS", "RESPONSE");
			jObject.put("CONTENT", jContent);
			jObject.put("FROM", jHeader);
			jObject.put("TYPE", "HIGHEST_SEQ_NUM");

			// String key = (String) ((JSONObject) jobj.get("FROM"))
			// .get("UNIQUENAME");
			Node replyTo = new Node((JSONObject) jobj.get("FROM"));
			synchronized (this) {

				nodes.put(replyTo.getName(), replyTo);
				sendStuff(jObject, replyTo);
			}
			// sendStuff(jObject, nodes.get(key));

		} else if (status.equals("RESPONSE")) {
			long tmp_max_num = (long) ((JSONObject) jobj.get("CONTENT"))
					.get("VALUE");
			synchronized (this) {
				--this.initCount;
				if (tmp_max_num > this.sequenceNumber) {
					this.sequenceNumber = (int) tmp_max_num;
				}
				if (this.initDone == false && this.initCount <= 0) {
					this.initDone = true;
					try {
						notifyAll();
					} catch (Exception e) {
						
					}
					System.out.println("init done " + this.sequenceNumber);
				}
			}
		} else
			System.out.println("Protocor error: " + jobj);
	}

	private void handleAreYouThere(JSONObject jobj) {

		JSONObject jObject = new JSONObject();
		JSONObject jHeader = prepareHeader();
		JSONObject jContent = new JSONObject();

		jObject.put("CONTENT", jContent);
		jObject.put("FROM", jHeader);
		jObject.put("TYPE", "YES_I_AM_HERE");
		// String key = (String) ((JSONObject)
		// jobj.get("FROM")).get("UNIQUENAME");
		Node replyTo = new Node((JSONObject) jobj.get("FROM"));
		// nodes.put(replyTo.getName(), replyTo);
		sendStuff(jObject, replyTo);
		// sendStuff(jObject, nodes.get(key));
	}

	private void handleReply(JSONObject jobj) {
		synchronized (this) {
			++repliesCount;

			Object o = new Object();
			if (initDone == true) {
				notifyAll();
			}

			if (repliesCount > nodes.size())
				System.out.println("Protocor error: to many replies");
		}

	}

	private void handleRequest(JSONObject jobj) {
		int hisSeqNum = (int) ((long) ((JSONObject) jobj.get("CONTENT"))
				.get("SEQNUM"));
		int mySeqNum = this.sequenceNumber;
		boolean canReply = false;

		while (canReply == false) {
			synchronized (this) {

				if (hasToken == true)
					canReply = false;
				else if (hasToken == false && needsToken == false)
					canReply = true;
				else if (needsToken == true && hisSeqNum < mySeqNum)
					canReply = true;
				else if (this.thisNode.getName().compareTo(
						((String) ((JSONObject) jobj.get("FROM"))
								.get("UNIQUENAME"))) > 0)
					canReply = true;
				else {
					try {
						wait();

					} catch (InterruptedException e) {
					}
				}
			}
			// try {
			// // TODO przerobic na semafor
			// Thread.sleep(10);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
		}

		JSONObject jObject = new JSONObject();
		JSONObject jHeader = prepareHeader();
		JSONObject jContent = new JSONObject();

		jObject.put("CONTENT", jContent);
		jObject.put("FROM", jHeader);
		jObject.put("TYPE", "REPLY");
		String key = (String) ((JSONObject) jobj.get("FROM")).get("UNIQUENAME");
		synchronized (this) {
			sendStuff(jObject, nodes.get(key));
		}

		// synchronized (this) {
		if (hisSeqNum > this.sequenceNumber) {
			this.sequenceNumber = (int) hisSeqNum;
			// }
		}

	}

	private synchronized void handleInit(JSONObject obj) {
		String role = (String) ((JSONObject) obj.get("CONTENT")).get("ROLE");
		if (role.equals("SPONSOR")) {
			Node sponsor = new Node((JSONObject) obj.get("FROM"));
			JSONObject jnodes = (JSONObject) ((JSONObject) obj.get("CONTENT"))
					.get("NODESDATA");

			Set keys = jnodes.keySet();
			Iterator iterator = keys.iterator();

			while (iterator.hasNext()) {
				String key = (String) iterator.next();
				JSONObject jobj = (JSONObject) jnodes.get(key);
				nodes.put(key, (new Node(jobj, key)));
			}

			// JSONObject jObject = new JSONObject();
			JSONObject jHeader = prepareHeader();
			// JSONObject jcontent = new JSONObject();
			// jcontent.put("Role", "Node");
			// jcontent.put("NewData", jHeader);
			// jObject.put("CONTENT", jcontent);
			// jObject.put("FROM", jHeader);
			// jObject.put("TYPE", "INIT");
			// sendToAll(jObject);

			nodes.put(sponsor.getName(), sponsor);
			initCount = nodes.size();
			// try {
			// Thread.sleep(1000);
			// } catch (InterruptedException e) {
			//
			// e.printStackTrace();
			// }

			JSONObject jObject2 = new JSONObject();
			JSONObject jcontent2 = new JSONObject();
			// jcontent2.put("Role", "Node");
			jcontent2.put("STATUS", "GET");
			jObject2.put("CONTENT", jcontent2);
			jObject2.put("FROM", jHeader);
			jObject2.put("TYPE", "HIGHEST_SEQ_NUM");
			sendToAll(jObject2, new Runnable() {
				@Override
				public void run() {
					doAfterGetMaxTimeOut();
				}
			}, maxUseTime);

		} else if (role.equals("NODE")) {
			// ktos nowy sie pojawil
			JSONObject jnewNode = (JSONObject) ((JSONObject) obj.get("CONTENT"))
					.get("NEWDATA");
			Node newOne = new Node(jnewNode);
			nodes.put(newOne.getName(), newOne);

		} else if (role.equals("NEW")) {

			JSONObject jObject1 = new JSONObject();
			JSONObject jHeader1 = prepareHeader();
			JSONObject jcontent1 = new JSONObject();
			jcontent1.put("ROLE", "NODE");
			jcontent1.put("NEWDATA", (JSONObject) obj.get("FROM"));
			jObject1.put("CONTENT", jcontent1);
			jObject1.put("FROM", jHeader1);
			jObject1.put("TYPE", "INIT");
			sendToAll(jObject1, null, 0);

			JSONObject jObject = new JSONObject();
			JSONObject jHeader = prepareHeader();
			JSONObject jcontent = new JSONObject();
			jcontent.put("ROLE", "SPONSOR");
			JSONObject jnodes = new JSONObject();
			// jnodes.
			for (Node n : nodes.values()) {
				jnodes.put(n.getName(), n.toJson());
			}

			Node jnewNode = new Node((JSONObject) obj.get("FROM"));
			nodes.put(jnewNode.getName(), jnewNode);
			jcontent.put("NODESDATA", jnodes);
			jcontent.put("STATUS", "OK");
			jObject.put("CONTENT", jcontent);
			jObject.put("FROM", jHeader);
			jObject.put("TYPE", "INIT");

			sendStuff(jObject, jnewNode);

		} else
			System.out.println("Protocor error: " + obj);

	}

	private JSONObject prepareHeader() {
		if (this.jHeaderObj == null) {
			synchronized (this) {
				if (this.jHeaderObj == null) {
					jHeaderObj = new JSONObject();
					jHeaderObj.put("Ip", thisNode.getAddress());
					jHeaderObj.put("Port", thisNode.getPort());
					jHeaderObj.put("UniqueName", thisNode.getName());
				}
			}
		}
		return jHeaderObj;
	}

	private synchronized void sendToAll(JSONObject stuff,
			Runnable taskAterTimeout, int timeOut) {
		Collection<Node> c = nodes.values();
		for (Node node : c) {
			send(node.getAddress(), node.getPort(), stuff.toString());
			if (taskAterTimeout != null) {
				setUpTimer(node, timeOut, taskAterTimeout);
			}
		}
	}

	// private synchronized void sendToAllWithTimer(JSONObject stuff) {
	//
	// }

	private synchronized void sendStuff(JSONObject stuff, Node node) {

		send(node.getAddress(), node.getPort(), stuff.toString());

	}

	private synchronized void send(String initHostAddress, int port,
			String stuff) {
		stuff = stuff.toUpperCase();
		try {
			Socket socket = new Socket(InetAddress.getByName(initHostAddress),
					port);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			out.println(stuff);
			out.close();
			socket.close();
			if (verbose)
				System.out.println("sending to:" + initHostAddress + ":" + port
						+ "   stuff: " + stuff);
			// System.out.println("to: " + initHostAddress + "  " + port);

		} catch (IOException e) {
			if (verbose)
				e.printStackTrace();
		}
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
		// final Socket clientSocket;// = null;
		while (true) {
			try {
				final Socket clientSocket = serverSocket.accept();

				BufferedReader in = new BufferedReader(new InputStreamReader(
						clientSocket.getInputStream()));

				final String inputLine = in.readLine();

				new Thread(new Runnable() {
					public void run() {
						System.out.println("received: client addr: "
								+ clientSocket.getRemoteSocketAddress()
								+ "  port: " + clientSocket.getPort()
								+ inputLine);
						dispatchInput(inputLine);
					}
				}).start();

				in.close();
				clientSocket.close();

			} catch (IOException e) {
				if (verbose)
					e.printStackTrace();
			}
		}
	}

	private void doDie() {
		// TODO sparametryzowac sleep - nieco dluzej niz mozna uzywac tokentu
		System.out.println("I hate this world... suiciding...");

		initDone = false;

		try {
			// TODO przerobic na semafor
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			if (verbose)
				e.printStackTrace();
		}

		JSONObject jObject = new JSONObject();
		JSONObject jHeader = prepareHeader();
		JSONObject jcontent = new JSONObject();
		jcontent.put("Status", "REMOVE");
		jObject.put("CONTENT", jcontent);
		jObject.put("FROM", jHeader);
		jObject.put("TYPE", "DEAD");

		sendToAll(jObject, null, 0);

		try {
			// TODO przerobic na semafor
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			if (verbose)
				e.printStackTrace();
		}

		System.out.println("I hate my coffin...");
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			doDie();
		} finally {
			super.finalize();
		}
	}

	// synchronized??
	public void requestToken() throws Exception {

		synchronized (this) {
			while (!initDone) {
				try {
					// TODO przerobic na semafor
					wait();
				} catch (InterruptedException e) {
					// e.printStackTrace();
				}
			}
		}

		this.needsToken = true;
		this.repliesCount = 0;

		synchronized (this) {
			++sequenceNumber;
		}

		JSONObject jObject = new JSONObject();
		JSONObject jHeader = prepareHeader();
		JSONObject jContent = new JSONObject();
		jContent.put("SeqNum", this.sequenceNumber);
		jObject.put("CONTENT", jContent);
		jObject.put("FROM", jHeader);
		jObject.put("TYPE", "REQUEST");
		synchronized (this) {
			sendToAll(jObject, new Runnable() {
				@Override
				public void run() {
					doAfterRequestTimeOut();
				}
			}, 10 * (nodes.size() + 2) * 1000);

		}

		boolean got = false;
		int size;
		synchronized (this) {
			size = nodes.size();
		}
		while (!got) {
			synchronized (this) {
				if (size == repliesCount) {
					got = true;
				} else {
					try {
						wait();
					} catch (InterruptedException e) {
					}
				}

				if (repliesCount == BAD_VALUE) {
					releaseToken();
					throw new Exception("node died, God knows what happened");
				}
				// try {
				// // TODO przerobic na semafor
				// Thread.sleep(10);
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }
			}
		}
		hasToken = true;
	}

	// synchronized??
	public synchronized void releaseToken() {
		// System.out.println("releaseToken not imlemented");
		hasToken = false;
		needsToken = false;
		notifyAll();
	}

	private void setUpTimer(final Node node, int timeInMilis,
			final Runnable taskAfterTimeout) {
		try {
			// timers = new ConcurrentHashMap<String, LinkedList<TimerTask>>();
			// tasks = new ConcurrentHashMap<String, LinkedList<Runnable>>();
			final String nodeName = node.getName();
			synchronized (timerLockObj) {
				LinkedList<TimerTask> timerList = timerLists.get(nodeName);
				if (timerList == null) {
					timerList = new LinkedList<TimerTask>();
					timerLists.put(node.getName(), timerList);

				}

				final LinkedList<TimerTask> timerListFin = timerList;

				TimerTask task = new TimerTask() {
					public void run() {
						doAreYouThere(nodeName);

						TimerTask taskAfter = new TimerTask() {
							public void run() {
								taskAfterTimeout.run();
								doNodeDied(node.getName());
							}
						};
						synchronized (timerLockObj) {
							timerListFin.add(taskAfter);
						}
						timer.schedule(taskAfter, 10 * 1000);
					}
				};

				timerListFin.add(task);

				timer.schedule(task, timeInMilis);
				
				System.out.println("new Timer: " + node.getName() + " time: "
						+ timeInMilis / 1000);
			}
		} catch (Exception e) {
			if (verbose)
				e.printStackTrace();
		}
	}

	private void cancelTimer(Node node) {

		final String nodeName = node.getName();
		synchronized (timerLockObj) {
			LinkedList<TimerTask> timerList = timerLists.get(nodeName);
			if (timerList != null) {
				for (TimerTask timerTask : timerList) {
					timerTask.cancel();
					System.out.println("Canceled timer: " + node.getName());
				}
				timerList.clear();
			}
		}
	}

	private void doNodeDied(String name) {
		System.out.println("doNodeDied: " + name);
		synchronized (this) {
			nodes.remove(name);
			notifyAll();
		}

	}

	private void doAreYouThere(String key) {
		int i = 0;
		i++;
		System.out.println("doAreYouThere: " + key);

		JSONObject jObject = new JSONObject();
		JSONObject jHeader = prepareHeader();
		JSONObject jContent = new JSONObject();
		jObject.put("CONTENT", jContent);
		jObject.put("FROM", jHeader);
		jObject.put("TYPE", "ARE_YOU_THERE");
		synchronized (this) {
			sendStuff(jObject, nodes.get(key));
		}

	}

	private void doAfterRequestTimeOut() {
		synchronized (this) {
			repliesCount = BAD_VALUE;
			notifyAll();
		}

		System.out.println("doAfterRequestTimeOut");
	}

	private void doAfterGetMaxTimeOut() {
		synchronized (this) {
			--initCount;
		}
		System.out.println("doAfterGetMaxTimeOut");
	}

	private static class MsgType {
		// TODO pozamieniac gdzie sie da wartosci "STRING" na te sta³e st¹d:
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
			aMap.put(INIT.toUpperCase(), 0);
			// aMap.put(REMOVE.toUpperCase(), 1);
			aMap.put(REQUEST.toUpperCase(), 2);
			aMap.put(REMOVE.toUpperCase(), 3);
			aMap.put(REPLY.toUpperCase(), 4);
			aMap.put(ARE_YOU_THERE.toUpperCase(), 5);
			aMap.put(YES_I_AM_HERE.toUpperCase(), 6);
			aMap.put(HIGHEST_SEQ_NUM.toUpperCase(), 7);
			aMap.put(DEAD.toUpperCase(), 8);
			handlerMap = Collections.unmodifiableMap(aMap);
		}
	}
}

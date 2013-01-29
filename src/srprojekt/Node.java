package srprojekt;

import org.json.simple.JSONObject;

public class Node {
	private String name;
	private int port;
	private String address;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public Node(String name, int port, String address) {
		super();
		this.name = name;
		this.port = port;
		this.address = address;
	}
	
	public Node() {
		super();

	}
	public Node(JSONObject object) {
		String uName = object.get("UNIQUENAME").toString();
		if(uName != null)
		{
			this.name = uName;
		}
		else
		{
			
		}
		this.port = Integer.parseInt(object.get("PORT").toString() );
		this.address = object.get("IP").toString();
	}
	
	public Node(JSONObject object, String name) {
		this.name = name;
		this.port = Integer.parseInt(object.get("PORT").toString() );
		this.address = object.get("IP").toString();
	}
	
	public JSONObject toJson ()
	{
		//JSONObject jobj = new JSONObject();
		
		JSONObject jinner = new JSONObject();
		jinner.put("IP",this.address);
		jinner.put("PORT",this.port);
		return jinner;
	//	jobj.put(this.name, jinner);
	//	return jobj;
	}
	
	public JSONObject toJsonWithName ()
	{
		//JSONObject jobj = new JSONObject();		
		JSONObject jinner = new JSONObject();
		jinner.put("UNIQUENAME",this.name);
		jinner.put("IP",this.address);
		jinner.put("PORT",this.port);
		return jinner;
	//	jobj.put(this.name, jinner);
	//	return jobj;
	}
	
}

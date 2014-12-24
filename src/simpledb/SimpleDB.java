package simpledb;
import java.util.Set;
import java.util.TreeMap;
import java.util.Iterator;

public class SimpleDB {

	boolean debugOn;
	
	public SimpleDB() {
		debugOn = false;
	}
	
	public SimpleDB(boolean debugflag) {
		debugOn = debugflag;
	}
	//core storage map
    private TreeMap<String, String> internalStorage = new TreeMap<String, String>();
    
    //index for fast count lookups
    private TreeMap<String, Integer> internalStorageCounts = new TreeMap<String, Integer>();
    
    
	public String get(String key) {
		return internalStorage.get(key);
	}

	public int getInternalStorageSize() {
		return internalStorage.size();
	}
	
	protected void storeValueAndCreateIndex(String key, String value) {
		//unset and clear index
		unset(key);
		
		//set value
		debug("SETTING: "+key+" to:"+value);
		internalStorage.put(key, value);
		
		//increment index
		Integer currentCount = new Integer(0);
		if (internalStorageCounts.get(value)!=null)	
			currentCount = internalStorageCounts.get(value);
		
		int incrementedCount = currentCount+1;
		debug("building index for: " + value + " count:" + incrementedCount );
		internalStorageCounts.put(value, incrementedCount);
	}
	
	
	public void set(String key, String value) {
		storeValueAndCreateIndex(key,value);
	}
	
	public void unset(String key) {
		//decrement index
		debug("unset");
		removeValueAndDecrementIndex(key);
	}

	protected void removeValueAndDecrementIndex(String key) {
		String value = internalStorage.get(key);
		debug("key="+key+" value="+value);
		if (value!=null) {
			if (internalStorageCounts.get(value)!=null) {
				debug("decrement index for: " + value + " count " + internalStorageCounts.get(value));
				internalStorageCounts.put(value, internalStorageCounts.get(value)-1);
			}
		}
		//un-set value
		internalStorage.remove(key);
	}
	
	public int countNumEqualTo(String key) {		
		return internalStorageCounts.get(key);
	}

	
	public void dumpDBIndex() {
		debug("DUMPING DB INDEX");

		Set keys = internalStorageCounts.keySet();
	    for (Iterator i = keys.iterator(); i.hasNext();) {
	      String key = (String) i.next();
	      Integer value = (Integer) internalStorageCounts.get(key);
	      debug(key + " = " + value);
	    }

	}
	
	protected void dumpDB() {
		debug("DUMPING DB");

		Set keys = internalStorage.keySet();
	    for (Iterator i = keys.iterator(); i.hasNext();) {
	      String key = (String) i.next();
	      String value = (String) internalStorage.get(key);
	      debug(key + " = " + value);
	    }

	}
	
	protected void debug(String debug) {
		if (debugOn)
			System.out.println(debug);
	}

}

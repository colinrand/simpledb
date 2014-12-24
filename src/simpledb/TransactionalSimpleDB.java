package simpledb;
import java.util.Stack;

public class TransactionalSimpleDB extends SimpleDB {

    public TransactionalSimpleDB(boolean debugflag) {
		super(debugflag);
		
	}

    public TransactionalSimpleDB() {
		super(false);
		
	}
	private Stack<Stack> changeStack = new Stack<Stack>();
    
	public void beginTransaction() {
		 Stack<Object[]> transactionStack = new Stack<Object[]>();
	     changeStack.push(transactionStack);
	}
	
	public void unset(String key) {
		if (!changeStack.isEmpty()) {
			debug("UNSET: " +key);

			//get current value (rollback value)
			String rollbackValue = get(key);
			debug ("UNSET ROLLBACKVALUE for key:" + key + " " +rollbackValue);
			if (rollbackValue != null) {
				
				Object[] rollbackCommand = {"SET", key, rollbackValue};
				debug("Pushing rollback command: SET "+ key + " " + rollbackValue);
				changeStack.peek().push(rollbackCommand);
				
			}
		}
		//call unset
		removeValueAndDecrementIndex(key);
	}
	public void set(String key, String value) {
		
		if (!changeStack.isEmpty()) {
			debug("SET: " +key+" "+value);
			debug("in txn set. changeStack size="+changeStack.size());
			//get current value (rollback value)
			String rollbackValue = get(key);
			
			//create rollback command
			if (rollbackValue != null) {
				
				
				Object[] rollbackCommand = {"SET", key, rollbackValue};
				debug("Pushing rollback command: SET "+ key + " " + rollbackValue);
				changeStack.peek().push(rollbackCommand);
				
				Object[] rollbackIndex = {"UNSET", key};
				debug("Pushing rollback index: UNSET "+ key);
				changeStack.peek().push(rollbackIndex);
	
			} else {
				Object[] rollbackCommand = {"UNSET", key};
				changeStack.peek().push(rollbackCommand);
				debug("Pushing rollback command: UNSET "+ key);			

			}
			
		}

		//save new value
        storeValueAndCreateIndex(key,value);
	}
	
	public int countCurrentChanges() {
		if (!changeStack.isEmpty()) {
			return changeStack.peek().size();
		}
		else return 0;
	}

	public void rollback() {
		debug("doing rollback");
		if (!changeStack.isEmpty()) {
			Stack<Object[]> commandsToRollback = changeStack.pop();
			Object[] command;
			
			
			while (commandsToRollback.size() !=0) {
				command = commandsToRollback.pop();
				String commandString = (String)command[0];
				debug("rollback command:" + commandString);
				if (commandString.compareTo("SET")==0) {
					debug("set key: "+ (String)command[1]);
					debug("to value: "+ (String)command[2]);
					set((String)command[1],(String)command[2]);
					
				} else {
					
					debug("unset key: "+ (String)command[1]);
					unset((String)command[1]);
				}
			}
		
		} else {
			System.out.println("NO TRANSACTION");
		}
	}

	public void commit() {
		changeStack = new Stack<Stack>(); //nuke it
		
	}

}

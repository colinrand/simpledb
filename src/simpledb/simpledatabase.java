package simpledb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class simpledatabase {

	public static void main(String[] args) {
		//create simpledatabase
		TransactionalSimpleDB simpleDB = new TransactionalSimpleDB();
		
		try{
			BufferedReader bufferedInput = new BufferedReader(new InputStreamReader(System.in));
			

			String inputline;
			String[] inputArgs;
			String command;
			
			while((inputline=bufferedInput.readLine())!=null){
				inputArgs = inputline.split(" ");
				command = inputArgs[0];
				switch (command) {
					case "SET":
						simpleDB.set(inputArgs[1], inputArgs[2]);
						break;
						
					case "UNSET":
						simpleDB.unset(inputArgs[1]);
						break;
						
					case "GET":
						System.out.println(simpleDB.get(inputArgs[1]));
						break;
						
					case "NUMEQUALTO":
						System.out.println(simpleDB.countNumEqualTo(inputArgs[1]));
						break;
						
					case "BEGIN":
						simpleDB.beginTransaction();
						break;
						
					case "ROLLBACK":
						simpleDB.rollback();
						break;
						
					case "COMMIT":
						simpleDB.commit();
						break;
						
					case "END":
						System.out.println("BYE BYE");
						System.exit(0);
						
					default:
						System.out.println("Command not recognized");
						break;
				}
				
			}
		}catch(IOException io){
			io.printStackTrace();
		}
	}

}

package simpledb;

import static org.junit.Assert.*;

import org.junit.Test;

public class TransactionalSimpleDBTest {

	@Test
	public void testCheckChangesAreTrackedAfterBeginTxn() {
		
		TransactionalSimpleDB tdb = new TransactionalSimpleDB(true);
		tdb.set("one","1");
		tdb.set("two", "2");
		tdb.set("three", "1");
		assertEquals("no changes tracked", 0, tdb.countCurrentChanges());
		
		tdb.beginTransaction();
		tdb.set("four","1");
		tdb.set("five", "2");
		tdb.set("six", "1");
		
//		System.out.println(tdb.countCurrentChanges());
		assertEquals("should be 3 changes tracked", 3, tdb.countCurrentChanges());

		tdb.beginTransaction();
		tdb.set("22","1");
		tdb.set("23", "2");
		tdb.set("24", "1");
		tdb.set("25", "2");
		tdb.set("26", "1");
		assertEquals("should be 5 changes tracked", 5, tdb.countCurrentChanges());
	
	}
	
	@Test
	public void testRollbackTransaction() {
		
		System.out.println("testRollbackTransaction");
		TransactionalSimpleDB tdb = new TransactionalSimpleDB(true);
		tdb.set("one","1");
		tdb.set("four","3");

		tdb.beginTransaction();
		tdb.set("four","1");
		tdb.set("five", "2");
		tdb.set("six", "1");

		tdb.dumpDB();
		tdb.dumpDBIndex();
		
		assertEquals(0,tdb.countNumEqualTo("3"));
		
		tdb.rollback();
		
		tdb.dumpDB();
		tdb.dumpDBIndex();
		
		assertEquals("should be 0 changes tracked", 0, tdb.countCurrentChanges());
		assertEquals("should be 2 item in db", 2, tdb.getInternalStorageSize());
		assertEquals("value for 4","3",tdb.get("four"));
		
		assertEquals(1,tdb.countNumEqualTo("1"));
		assertEquals(1,tdb.countNumEqualTo("3"));
	}
	
	@Test
	public void testCallUnsetFromWithinTransaction() {
		
		System.out.println("testCallUnsetFromWithinTransaction");
		TransactionalSimpleDB tdb = new TransactionalSimpleDB(true);
		tdb.set("a","50");
		tdb.beginTransaction();

		tdb.unset("a");
		assertEquals(null,tdb.get("a"));
		
		tdb.rollback();
		assertEquals("50",tdb.get("a"));		
	}
	
	@Test
	public void testTwoRollbacks() {
		System.out.println("testTwoRollbacks");
		TransactionalSimpleDB tdb = new TransactionalSimpleDB(true);
		tdb.set("one","1");
		tdb.set("three","3");

		tdb.beginTransaction();
		tdb.set("four","1");
		tdb.set("five", "1");
		tdb.set("six", "1");
		
		tdb.beginTransaction();
		tdb.set("four","4");
		tdb.set("five", "5");
		
		assertEquals("4",tdb.get("four"));
		assertEquals("5",tdb.get("five"));
		
		tdb.rollback();
		
		assertEquals("1",tdb.get("four"));
		assertEquals("1",tdb.get("five"));
		
		tdb.rollback();
		assertEquals(null,tdb.get("four"));
		
		
	}
	
	@Test
	public void testCommitTransaction() {
		TransactionalSimpleDB tdb = new TransactionalSimpleDB(true);
		tdb.set("one","1");
		tdb.set("three","3");

		tdb.beginTransaction();
		tdb.set("four","1");
		tdb.set("five", "1");
		tdb.set("six", "1");
		
		tdb.commit();
		
		tdb.rollback(); //should be noop
		assertEquals("1",tdb.get("four"));
		
	}
	
	public void testNestedTransactions() {
		fail("Not yet implemented");
		
	}

}

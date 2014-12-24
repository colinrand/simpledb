package simpledb;

import static org.junit.Assert.*;

import org.junit.Test;

public class SimpleDBTest {

	public void test() {
		assertTrue("hello",true);
	}  
	
	
	
	public void testSet() {
		fail("set not implemented");
	}
	
	@Test
	public void testStaticGet() {
		SimpleDB simpleDb = new SimpleDB(true);
		simpleDb.set("one", "one");
		simpleDb.set("two", "two");
		
		assertEquals("one",simpleDb.get("one"));
		assertEquals("two",simpleDb.get("two"));		
	}
	
	@Test
	public void testGetKeyNotFoundReturnsNull() {
		SimpleDB simpleDb = new SimpleDB(true);
		assertEquals("null return value",null,simpleDb.get("three"));
	}
	
	@Test
	public void testUnset() {
		SimpleDB simpleDb = new SimpleDB(true);
		simpleDb.set("one", "one");
		simpleDb.set("two", "two");
		
		assertEquals("one",simpleDb.get("one"));
		assertEquals("two",simpleDb.get("two"));		
		
		simpleDb.unset("one");
		assertEquals(null,simpleDb.get("one"));
		assertEquals("two",simpleDb.get("two"));		
		
	}
	
	@Test
	public void testNumEqualTo() {
		SimpleDB simpleDb = new SimpleDB(true); 
		simpleDb.set("1", "one");
		simpleDb.set("2", "two");
		
		assertEquals(1,simpleDb.countNumEqualTo("one"));
		assertEquals(1,simpleDb.countNumEqualTo("two"));		
		
		simpleDb.set("three", "one");
		assertEquals(2,simpleDb.countNumEqualTo("one"));
	
		simpleDb.set("four", "one");
		assertEquals(3,simpleDb.countNumEqualTo("one"));
		
		simpleDb.unset("1");
		assertEquals(2,simpleDb.countNumEqualTo("one"));
		
		
		
	}
	
	public void testEnd() {
		
	}
	
	public void testBegin() {
		
	}
	
	public void testRollback() {
		
	}
	
	public void testCommit() {
		
	}
}

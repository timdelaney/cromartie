package com.tim.cromartie;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

public class JobTest {

	@Test
	public void testSingleKey() {
		Job job = new Job();
		job.setKeyColumns("Col1");
		ArrayList<String> keyStrings = job.getKeyColumnList();
		assertTrue(keyStrings.get(0).equals("Col1"));
	}
	
	@Test
	public void testMultiKey() {
		Job job = new Job();
		job.setKeyColumns("Col1|Col2");
		ArrayList<String> keyStrings = job.getKeyColumnList();
		assertTrue(keyStrings.get(1).equals("Col2"));
	}
	
	@Test
	public void testKeyWithClose() {
		Job job = new Job();
		job.setKeyColumns("Col1|Col2|");
		ArrayList<String> keyStrings = job.getKeyColumnList();
		assertTrue(keyStrings.size()==2);
	}
	@Test
	public void testKeyWithSpaces() {
		Job job = new Job();
		job.setKeyColumns("Col1 | Col2");
		ArrayList<String> keyStrings = job.getKeyColumnList();
		assertTrue(keyStrings.size()==2);
		assertTrue(keyStrings.get(0).equals("Col1"));
		assertTrue(keyStrings.get(1).equals("Col2"));
	}
}

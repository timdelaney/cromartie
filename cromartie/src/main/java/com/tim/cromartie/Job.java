package com.tim.cromartie;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class Job {
	private String jobID;
	private String sourceSchema;
	private String sourceTable;
	private String targetSchema;
	private String targetTable;
	private String targetType;
	private String sourceEffectiveColumn;
	private String sourceExpirationColumn;
	private String targetEffectiveColumn;
	private String targetExpirationColumn ;
	private String KeyColumns;
	private String stimulateFlag;	
	private String active;
	private Date nextRun;
	private String runFrequency;

	public String getJobID() {
		return jobID;
	}
	public void setJobID(String jobID) {
		this.jobID = jobID;
	}
	public String getSourceSchema() {
		return sourceSchema;
	}
	public void setSourceSchema(String sourceSchema) {
		this.sourceSchema = sourceSchema;
	}
	public String getSourceTable() {
		return sourceTable;
	}
	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}
	public String getTargetSchema() {
		return targetSchema;
	}
	public void setTargetSchema(String targetSchema) {
		this.targetSchema = targetSchema;
	}
	public String getTargetTable() {
		return targetTable;
	}
	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}
	public String getTargetType() {
		return targetType;
	}
	public void setTargetType(String targetType) {
		this.targetType = targetType;
	}
	public String getSourceEffectiveColumn() {
		return sourceEffectiveColumn;
	}
	public void setSourceEffectiveColumn(String sourceEffectiveColumn) {
		this.sourceEffectiveColumn = sourceEffectiveColumn;
	}
	public String getSourceExpirationColumn() {
		return sourceExpirationColumn;
	}
	public void setSourceExpirationColumn(String sourceExpirationColumn) {
		this.sourceExpirationColumn = sourceExpirationColumn;
	}
	public String getTargetEffectiveColumn() {
		return targetEffectiveColumn;
	}
	public void setTargetEffectiveColumn(String targetEffectiveColumn) {
		this.targetEffectiveColumn = targetEffectiveColumn;
	}
	public String getTargetExpirationColumn() {
		return targetExpirationColumn;
	}
	public void setTargetExpirationColumn(String targetExpirationColumn) {
		this.targetExpirationColumn = targetExpirationColumn;
	}
	public String getKeyColumns() {
		return KeyColumns;
	}
	public void setKeyColumns(String keyColumns) {
		KeyColumns = keyColumns;
	}
	public String getStimulateFlag() {
		return stimulateFlag;
	}
	public void setStimulateFlag(String stimulateFlag) {
		this.stimulateFlag = stimulateFlag;
	}
	public String getActive() {
		return active;
	}
	public void setActive(String active) {
		this.active = active;
	}
	public Date getNextRun() {
		return nextRun;
	}
	public void setNextRun(Date nextRun) {
		this.nextRun = nextRun;
	}
	public String getRunFrequency() {
		return runFrequency;
	}
	public void setRunFrequency(String runFrequency) {
		this.runFrequency = runFrequency;
	}
	
	public ArrayList<String> getKeyColumnList(){
		return new ArrayList<String>(Arrays.asList(this.KeyColumns.split("\\s*\\|\\s*")));
	}
	
	
 
}

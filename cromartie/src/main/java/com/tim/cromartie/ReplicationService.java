package com.tim.cromartie;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ReplicationService {

    @Autowired
    private JdbcTemplate jdbcTemplate;
	private String sourceTable;
	private String targetTable;
	private String dtmKey;
	private String targetType;
	private String targetEffectiveColumn;
	private String targetExpirationColumn;
	private String sourceEffectiveColumn;
	private String sourceSchema;
	private String targetSchema;
	private String keyColumn;
	
	public String getSourceTable() {
		return sourceTable;
	}
	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}
	public String getTargetTable() {
		return targetTable;
	}
	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}
	public String getDtmKey() {
		return dtmKey;
	}
	public void setDtmKey(String dtmKey) {
		this.dtmKey = dtmKey;
	}
	public String getTargetType() {
		return targetType;
	}
	public void setTargetType(String targetType) {
		this.targetType = targetType;
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
	public String getSourceEffectiveColumn() {
		return sourceEffectiveColumn;
	}
	public void setSourceEffectiveColumn(String sourceEffectiveColumn) {
		this.sourceEffectiveColumn = sourceEffectiveColumn;
	}
	public String getSourceSchema() {
		return sourceSchema;
	}
	public void setSourceSchema(String sourceSchema) {
		this.sourceSchema = sourceSchema;
	}
	public String getTargetSchema() {
		return targetSchema;
	}
	public void setTargetSchema(String targetSchema) {
		this.targetSchema = targetSchema;
	}
	public String getKeyColumn() {
		return keyColumn;
	}
	public void setKeyColumn(String keyColumn) {
		this.keyColumn = keyColumn;
	}
	
	public void runJob(String sourceSchema, String sourceTable, String targetSchema, String targetTable,
			String targetType, String targetEffectiveColumn, String targetExpirationColumn,
			String sourceEffectiveColumn, ArrayList<String> keyColumns, String stimulate) throws SQLException {
		if(targetType.equals("historical")) {
    		executeHistoricalLoad(sourceTable, targetTable, targetEffectiveColumn, targetExpirationColumn,
    				sourceEffectiveColumn, sourceSchema, targetSchema, keyColumns, stimulate);
    	}else if(targetType.equals("physicalize")) {
    		physicalize(sourceTable, targetTable, sourceEffectiveColumn, targetSchema, stimulate);
    	}
	}
	
	   @Transactional
	    private void executeHistoricalLoad(String sourceTable, String targetTable, String targetEffectiveColumn,
				String targetExpirationColumn, String sourceEffectiveColumn, String sourceSchema, String targetSchema,
				ArrayList<String> keyColumns, String simulate) throws SQLException {
			
			String sql = null;
			
			sql = createExpireUpdateStatement(sourceTable, targetTable, targetEffectiveColumn, targetExpirationColumn,
					sourceEffectiveColumn, sourceSchema, targetSchema, keyColumns);
			executeStatement(sql, simulate);
			
			sql = createUpdateInsertStatement(sourceTable, targetTable, targetEffectiveColumn, targetExpirationColumn,
					sourceEffectiveColumn, sourceSchema, targetSchema, keyColumns);
			executeStatement(sql, simulate);

			sql = createInsertStatement(sourceTable, targetTable, targetEffectiveColumn, targetExpirationColumn,
					sourceEffectiveColumn, sourceSchema, targetSchema, keyColumns);
			executeStatement(sql, simulate);
		}
	 
	@Transactional
	private void physicalize(String sourceTable, String targetTable, String sourceEffectiveColumn, String targetSchema, String simulate) {
		String insert = "INSERT INTO " + targetSchema + targetTable + " SELECT * FROM test_db." + sourceTable 
    			+ " WHERE " + sourceEffectiveColumn + " > (SELECT COALESCE(MAX(" + sourceEffectiveColumn + "),'1900-01-01') FROM test_db." + targetTable +")";
    	
		executeStatement(insert, simulate);
	}
	
    private String createKeyColumnJoinString(ArrayList<String> keyColumns) {
    	StringBuilder keyColumnString = new StringBuilder();
    	for(String keyColumn: keyColumns) {
    		keyColumnString.append(" src.");
    		keyColumnString.append(keyColumn);
    		keyColumnString.append(" = ");
    		keyColumnString.append("tgt.");
    		keyColumnString.append(keyColumn);
    		keyColumnString.append(" AND ");
    	}
    	
    	keyColumnString.delete(keyColumnString.length()-4, keyColumnString.length());
    	return keyColumnString.toString();
    }
    
    private void executeStatement(String statement, String simulate) {
    	System.out.println(statement);
    	if(!simulate.equals("true")) {
    		jdbcTemplate.execute(statement);
    	}
    }
	

	
	private String createInsertStatement(String sourceTable, String targetTable, String targetEffectiveColumn,
			String targetExpirationColumn, String sourceEffectiveColumn, String sourceSchema, String targetSchema,
			ArrayList<String> keyColumns) throws SQLException {
		
		StringBuilder sb = new StringBuilder();
   	 	sb.append("INSERT INTO ");
    	 sb.append(targetSchema);
    	 sb.append(".");
    	 sb.append(targetTable);
    	 sb.append(" SELECT src.");
    	 sb.append(getColumnString(sourceTable, ", src.", targetSchema));
    	 sb.append(",");
    	 sb.append("'9999-12-31 00:00:00' FROM ");
    	 sb.append(sourceSchema);
    	 sb.append(".");
    	 sb.append(sourceTable);
    	 sb.append(" as src LEFT OUTER JOIN ");
    	 sb.append(targetSchema);
    	 sb.append(".");
    	 sb.append(targetTable);
    	 sb.append(" as tgt ON ");
    	 sb.append(createKeyColumnJoinString(keyColumns));
    	 sb.append(" WHERE ");
    	 
    	 for(String keyColumn: keyColumns) {
        	 sb.append(" tgt.");
        	 sb.append(keyColumn);
        	 sb.append(" IS NULL AND");
    	 }
     	 sb.delete(sb.length()-3, sb.length());

    	 return sb.toString();
	}

	private String createUpdateInsertStatement(String sourceTable, String targetTable, String targetEffectiveColumn,
			String targetExpirationColumn, String sourceEffectiveColumn, String sourceSchema, String targetSchema,
			ArrayList<String> keyColumns) throws SQLException {
		StringBuilder sb = new StringBuilder();
    	 sb.append("INSERT INTO ");
    	 sb.append(targetSchema);
    	 sb.append(".");
    	 sb.append(targetTable);
    	 sb.append(" SELECT src.");
    	 sb.append(getColumnString(sourceTable, ", src.",targetSchema));
    	 sb.append(",");
    	 sb.append("'9999-12-31 00:00:00' FROM ");
    	 sb.append(sourceSchema);
    	 sb.append(".");
    	 sb.append(sourceTable);
    	 sb.append(" as src,");
    	 sb.append(targetSchema);
    	 sb.append(".");
    	 sb.append(targetTable);
    	 sb.append(" as tgt");
    	 sb.append("  WHERE ");
    	 sb.append(createKeyColumnJoinString(keyColumns));
    	 sb.append(" AND ");
    	 sb.append(getChangeDetectionClause(targetTable,keyColumns, targetEffectiveColumn, targetExpirationColumn, targetSchema));
    	 sb.append(" AND tgt.");
    	 sb.append(targetExpirationColumn);
    	 sb.append(" = src.");
    	 sb.append(sourceEffectiveColumn);
    	 return sb.toString();
	}

	private String createExpireUpdateStatement(String sourceTable, String targetTable, String targetEffectiveColumn,
			String targetExpirationColumn, String sourceEffectiveColumn, String sourceSchema, String targetSchema,
			ArrayList<String> keyColumns) throws SQLException {
		StringBuilder sb = new StringBuilder();
    	 sb.append("UPDATE ");
    	 sb.append(targetSchema);
    	 sb.append(".");
    	 sb.append(targetTable);
    	 sb.append(" as tgt, ");
    	 sb.append(sourceSchema);
    	 sb.append(".");
    	 sb.append(sourceTable);
    	 sb.append(" as src SET ");
    	 sb.append("tgt.");
    	 sb.append(targetExpirationColumn);
    	 sb.append(" = ");
    	 sb.append("src.");
    	 sb.append(sourceEffectiveColumn);
    	 sb.append(" WHERE ");
    	 sb.append(createKeyColumnJoinString(keyColumns));
    	 sb.append(" AND tgt.");
    	 sb.append(targetExpirationColumn);
    	 sb.append(" ='9999-12-31 00:00:00' AND ");
    	 sb.append(getChangeDetectionClause(targetTable,keyColumns, targetEffectiveColumn, targetExpirationColumn, targetSchema));
    	 return sb.toString();

	}
    
	private String getChangeDetectionClause(String targetTable, ArrayList<String> keyColumns, String targetEffectiveColumn, String targetExpirationColumn, String targetSchema) throws SQLException {
		ArrayList<Column> columns = getColumns(targetTable, targetSchema);
        int i=0;
    	StringBuilder columnString=new StringBuilder();  
    	columnString.append("(");
    	for(Column column: columns) {
    		if( !keyColumns.contains(column.getName()) && !column.getName().equals(targetEffectiveColumn) && !column.getName().equals(targetExpirationColumn)) {
    			columnString.append(getChangeDetectionItem(column.getName()));
            		columnString.append(" OR ");
    		}
    	}
    	//remove final OR
    	columnString.delete(columnString.length()-4, columnString.length());
    	columnString.append(")");
		return columnString.toString();
	}
	
    private String getChangeDetectionItem(String col) throws SQLException {
    		StringBuilder columnString=new StringBuilder();  
    		columnString.append("( src.");
    		columnString.append(col);
    		columnString.append(" <> tgt.");
    		columnString.append(col);
    		columnString.append(" OR (src.");
    		columnString.append(col);
    		columnString.append(" IS NULL and tgt.");
    		columnString.append(col);
    		columnString.append(" IS NOT NULL) OR (src.");
    		columnString.append(col);
    		columnString.append(" IS NOT NULL and tgt.");
    		columnString.append(col);
    		columnString.append(" IS NULL)");
    		columnString.append(")");
    		
		return columnString.toString();
	}

	private String getColumnString(String targetTable, String delimiter, String schema) throws SQLException {
		ArrayList<Column> columns = getColumns(targetTable, schema);
        int i=0;
    	StringBuilder columnString=new StringBuilder();  
    	for(Column column: columns) {
    		
    		columnString.append(column.getName());
    		if(i++ != columns.size() - 1){
        		columnString.append(delimiter);
    	    }
    	}
		return columnString.toString();
	}

	private ArrayList<Column> getColumns(String table, String schema) throws SQLException {
		Connection con;
		con = jdbcTemplate.getDataSource().getConnection();
		
    	DatabaseMetaData metadata = con.getMetaData();
        ResultSet resultSet = metadata.getColumns(null, schema, table, null);
        ArrayList<Column> columns = new ArrayList<Column>();
        
        while (resultSet.next()) {
          Column column = new Column ();
          column.setName(resultSet.getString("COLUMN_NAME"));
          column.setType(resultSet.getString("TYPE_NAME"));
          column.setSize(resultSet.getInt("COLUMN_SIZE"));
          columns.add(column);
        }
        return columns;
	}





}

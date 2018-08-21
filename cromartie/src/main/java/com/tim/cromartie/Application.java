package com.tim.cromartie;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String args[]) {
        SpringApplication.run(Application.class, args);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    
    @Override
    public void run(String... strings) throws Exception {
    	//todo auditing
    	//todo config based processing
    	//todo endpoints for start/stop/status
    	//todo writing to stage
    	//todo pesimistic checks? Should I kick out bad rows and run anyway?
    	
    	String sourceSchema = "test_db";
    	String sourceTable = "tim_test";
    	String targetSchema = "test_db";
    	String targetTable = "tgt_timtest_hist";
    	String targetType = "historical";
    	String targetEffectiveColumn = "row_eff_dm";
    	String targetExpirationColumn = "row_exp_dm";
    	String sourceEffectiveColumn = "dte";
    	ArrayList<String> keyColumns = new ArrayList<String>();
    	keyColumns.add("id");
    	String stimulate = "true";
    	
    	if(targetType.equals("historical")) {
    		executeHistoricalLoad(sourceTable, targetTable, targetEffectiveColumn, targetExpirationColumn,
    				sourceEffectiveColumn, sourceSchema, targetSchema, keyColumns, stimulate);
    	}else if(targetType.equals("physicalize")) {
    		physicalize(sourceTable, targetTable, sourceEffectiveColumn, stimulate);
    	}
    	

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

	private void printTarget(String targetTable) {
		List<Map> rows  = (ArrayList) jdbcTemplate.queryForList("SELECT * FROM test_db."+targetTable);
    	for (Map row : rows) {
    		System.out.println(row.get("col1"));
    	}
	}

	private void physicalize(String sourceTable, String targetTable, String sourceEffectiveColumn, String simulate) {
		String insert = "INSERT INTO " + "test_db." + targetTable + " SELECT * FROM test_db." + sourceTable 
    			+ " WHERE " + sourceEffectiveColumn + " > (SELECT COALESCE(MAX(" + sourceEffectiveColumn + "),'1900-01-01') FROM test_db." + targetTable +")";
    	
		executeStatement(insert, simulate);
	}

	private void createReplicationTarget(String sourceTable, String targetTable, String schema) throws SQLException {
		Connection con;
		con = jdbcTemplate.getDataSource().getConnection();
    	DatabaseMetaData dbm = con.getMetaData();
    	ResultSet tables = dbm.getTables(null, schema, targetTable, null);
    	if (tables.next()) {
    		
    	}
    	else {
    		jdbcTemplate.execute("CREATE TABLE test_db."+targetTable+" LIKE " + "test_db."+sourceTable);
    	}
	}
}
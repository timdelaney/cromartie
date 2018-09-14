package com.tim.cromartie;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MetadataTest {

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ReplicationService replicationService;
    
    @Autowired
    MetadataService metadataService;
   
   
	@Test
	public void createMetadataStore() throws SQLException {
		
		metadataService.createMetadataStore("test_db");
		Connection con;
		con = jdbcTemplate.getDataSource().getConnection();
		
    	DatabaseMetaData metadata = con.getMetaData();
        ResultSet resultSet = metadata.getColumns(null, "test_db", "croma_data", null);
		
        assertTrue(resultSet.next()!=false);
		
   }
	
	@Test
	public void loadMetadata() throws SQLException {
		metadataService.createMetadataStore("test_db");
		
		jdbcTemplate.execute("DELETE FROM test_db.croma_data");
		jdbcTemplate.execute("INSERT INTO `test_db`.`croma_data`\n" + 
				"(`JobID`,\n" + 
				"`SourceSchema`,\n" + 
				"`SourceTable`,\n" + 
				"`TargetSchema`,\n" + 
				"`TargetTable`,\n" + 
				"`TargetType`,\n" + 
				"`SourceEffCol`,\n" + 
				"`SourceExpColumn`,\n" + 
				"`TargetEffColumn`,\n" + 
				"`TargetExpColumn`,\n" + 
				"`KeyColumns`,\n" + 
				"`SimulateFlag`,\n" + 
				"`Active`,\n" + 
				"`NextRun`,\n" + 
				"`RunFrequency`)\n" + 
				"VALUES\n" + 
				"(9999999,\n" + 
				"'test_db',\n" + 
				"'tim_test',\n" + 
				"'test_db',\n" + 
				"'tgt_timtest_hist',\n" + 
				"'historical',\n" + 
				"'row_eff_dm',\n" + 
				"'row_exp_dm',\n" + 
				"'row_eff_dm',\n" + 
				"'row_exp_dm',\n" + 
				"'id',\n" + 
				"'Y',\n" + 
				"'Y',\n" + 
				"'2015-01-01 00:00:00',\n" + 
				"'24m'\n" + 
				");\n" + 
				"");
		
		List<Job> jobs = metadataService.getJobs("test_db");
		assertTrue(null != jobs.get(0));
		jdbcTemplate.execute("DELETE FROM test_db.croma_data WHERE JobID = 9999999");

   }

	public void prep() throws SQLException {
	
	}

	public void cleanUp() {
	
	}
		
	


}


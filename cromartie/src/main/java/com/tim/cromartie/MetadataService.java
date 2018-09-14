package com.tim.cromartie;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class MetadataService {

	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	public void createMetadataStore(String database) throws SQLException {
		Connection con;
		con = jdbcTemplate.getDataSource().getConnection();
		
    	DatabaseMetaData metadata = con.getMetaData();
        ResultSet resultSet = metadata.getColumns(null, database, "croma_data", null);
        
        if(resultSet.next()==false) {
        	System.out.println("Creating Metadata Store in:" + database + "croma_data");
        	jdbcTemplate.execute("CREATE TABLE " + database + ".croma_data (\n" + 
        			"  `JobID` VARCHAR(135) NOT NULL,\n" + 
        			"  `SourceSchema` VARCHAR(135) NULL,\n" + 
        			"  `SourceTable` VARCHAR(135) NULL,\n" + 
        			"  `TargetSchema` VARCHAR(135) NULL,\n" + 
        			"  `TargetTable` VARCHAR(135) NULL,\n" + 
        			"  `TargetType` VARCHAR(135) NULL,\n" + 
        			"  `SourceEffCol` VARCHAR(135) NULL,\n" + 
        			"  `SourceExpColumn` VARCHAR(135) NULL,\n" + 
        			"  `TargetEffColumn` VARCHAR(135) NULL,\n" + 
        			"  `TargetExpColumn` VARCHAR(135) NULL,\n" + 
        			"  `KeyColumns` VARCHAR(135) NULL,\n" + 
        			"  `SimulateFlag` VARCHAR(135) NULL,\n" + 
        			"  `Active` VARCHAR(45) NULL,\n" + 
        			"  `NextRun` DATETIME NULL,\n" + 
        			"  `RunFrequency` VARCHAR(135) NULL,\n" + 
        			"  PRIMARY KEY (`JobID`));\n" + 
        			"");
        }else {
        	System.out.println("Metadata Store already exists.");
        }
		
		
	}

	public List<Job> getJobs(String database) {

		List<Job> jobs = jdbcTemplate.query("SELECT * FROM " + database + ".croma_data", 
				new BeanPropertyRowMapper(Job.class));
		return jobs;
	}

}

package com.tim.cromartie;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;

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
public class Type2Test {

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ReplicationService replicationService;
    
    @Autowired
    MetadataService metadataService;
   
	@Test
	public void type2() throws SQLException {

			prep();
			 
			//updateExpire
			 assertTrue("Expire Record Failed", jdbcTemplate.queryForObject(
					"SELECT \n" + 
							"	COUNT(*) \n" + 
							"FROM \n" + 
							"	test_db.tgt_timtest_hist\n" + 
							"WHERE \n" + 
							"	id = 1 AND col1 = 'ABC' \n" + 
							"and \n" + 
							"	row_eff_dm='2015-01-01 00:00:00' \n" + 
							"and \n" + 
							"	row_exp_dm = '2015-01-02'", new Object[] { }, Integer.class)==1);
			 
			 //updateInsert
			 assertTrue("Insert Update Record Failed", jdbcTemplate.queryForObject(
						"SELECT \n" + 
								"	COUNT(*) \n" + 
								"FROM \n" + 
								"	test_db.tgt_timtest_hist\n" + 
								"WHERE \n" + 
								"	id = 1 AND col1 = 'ABC' \n" + 
								"and \n" + 
								"	row_eff_dm='2015-01-01 00:00:00' \n" + 
								"and \n" + 
								"	row_exp_dm = '2015-01-02'", new Object[] { }, Integer.class)==1);
			
			 //insert
			 assertTrue("Insert New Record Failed",jdbcTemplate.queryForObject(
						"SELECT \n" + 
								"	COUNT(*) \n" + 
								"FROM \n" + 
								"	test_db.tgt_timtest_hist\n" + 
								"WHERE \n" + 
								"	id = 1 AND col1 = 'ABC' \n" + 
								"and \n" + 
								"	row_eff_dm='2015-01-01 00:00:00' \n" + 
								"and \n" + 
								"	row_exp_dm = '2015-01-02'", new Object[] { }, Integer.class)==1);
			cleanUp();

		   }

	public void prep() throws SQLException {
		//create source table
		jdbcTemplate.execute("CREATE TABLE `tim_test` (\n" + 
				"  `id` int(11) NOT NULL,\n" + 
				"  `col1` varchar(45) DEFAULT NULL,\n" + 
				"  `dte` datetime DEFAULT NULL,\n" + 
				"  PRIMARY KEY (`id`)\n" + 
				") ENGINE=InnoDB DEFAULT CHARSET=latin1;\n" + 
				"");
		
		//create target table
		jdbcTemplate.execute("CREATE TABLE `tgt_timtest_hist` (\n" + 
				"  `id` int(11) NOT NULL,\n" + 
				"  `col1` varchar(45) DEFAULT NULL,\n" + 
				"  `row_eff_dm` datetime NOT NULL,\n" + 
				"  `row_exp_dm` datetime DEFAULT NULL,\n" + 
				"  PRIMARY KEY (`id`,`row_eff_dm`)\n" + 
				") ENGINE=InnoDB DEFAULT CHARSET=latin1;\n" + 
				"");
		
		//insert target record for update
		jdbcTemplate.execute("INSERT INTO `test_db`.`tgt_timtest_hist`\n" + 
				"(`id`,\n" + 
				"`col1`,\n" + 
				"`row_eff_dm`,\n" + 
				"`row_exp_dm`)\n" + 
				"VALUES\n" + 
				"(1,\n" + 
				"'ABC',\n" + 
				"'2015-01-01 00:00:00',\n" + 
				"'9999-12-31 00:00:00');\n" + 
				"");
		
		//insert source record for update
		jdbcTemplate.execute("INSERT INTO `test_db`.`tim_test`\n" + 
				"(`id`,\n" + 
				"`col1`,\n" + 
				"`dte`)\n" + 
				"VALUES (\n" + 
				"1,\n" + 
				"'XYZ',\n" + 
				"'2015-01-02 00:00:00')\n" + 
				"");
		
		//insert source record for insert
		jdbcTemplate.execute("INSERT INTO `test_db`.`tim_test`\n" + 
				"(`id`,\n" + 
				"`col1`,\n" + 
				"`dte`)\n" + 
				"VALUES (\n" + 
				"2,\n" + 
				"'LMN',\n" + 
				"'2015-01-02 00:00:00')\n" + 
				"");
		
		ArrayList<String> keyColumns = new ArrayList<String>();
		keyColumns.add("id");
		
		replicationService.runJob("test_db", "tim_test", "test_db", "tgt_timtest_hist", "historical",
				"row_eff_dm", "row_exp_dm", "dte", keyColumns, "false");
	}

	public void cleanUp() {
		//drop source table
		jdbcTemplate.execute("DROP TABLE test_db.tim_test");
		
		//drop target table
		jdbcTemplate.execute("DROP TABLE test_db.tgt_timtest_hist");
	}
		
	


}


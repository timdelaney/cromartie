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

    @Autowired
    ReplicationService replicationService;
    
    @Override
    public void run(String... strings) throws Exception {
    	//todo auditing
    	//todo config based processing
    	//todo endpoints for start/stop/status
    	//todo writing to stage
    	//todo pesimistic checks? Should I kick out bad rows and run anyway?
    	
    	/*String sourceSchema = "test_db";
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
    	*/

    }
}
package com.gratex.gendao.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * This class was generated from database by the gen-dao tool
 * 
 * @author martom
 *
 */
public class Database implements AutoCloseable {

	private static final Logger LOG = Logger.getAnonymousLogger();

	private Connection connection;

	public Database() {
		Properties connectionProps = new Properties();
		try {
			File connectionPropsFile = new File(System.getProperty("user.dir") + "/../src/main/resources/db/connection.properties");
			if (!connectionPropsFile.exists()) {
				throw new FileNotFoundException("connection.properties not found in " + connectionPropsFile);
			}
			connectionProps.load(new BufferedReader(new FileReader(connectionPropsFile)));
//			System.out.println(connectionPropsFile);
//			connectionProps.load(getClass().getClassLoader().getResourceAsStream("db/db.properties"));
			this.connection = DriverManager.getConnection(
					connectionProps.getProperty("db.url"),
					connectionProps.getProperty("db.usr"),
					connectionProps.getProperty("db.pwd"));
		} catch (IOException ioe) {
			LOG.severe(ioe.getMessage());
		} catch (SQLException sqle) {
			LOG.severe(sqle.getMessage());
		}
	}

	public Select select(String... columns) {
		return new Select(connection, columns);
	}

	@Override
	public void close() {
		try {
			if (this.connection != null && !this.connection.isClosed()) {
				this.connection.close();
			}
		} catch (SQLException sqle) {
			LOG.severe("database connection could not be closed");
		}
	}
}

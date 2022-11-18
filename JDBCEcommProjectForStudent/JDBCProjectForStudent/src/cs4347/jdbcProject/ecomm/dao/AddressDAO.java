/* NOTICE: All materials provided by this project, and materials derived 
 * from the project, are the property of the University of Texas. 
 * Project materials, or those derived from the materials, cannot be placed 
 * into publicly accessible locations on the web. Project materials cannot 
 * be shared with other project teams. Making project materials publicly 
 * accessible, or sharing with other project teams will result in the 
 * failure of the team responsible and any team that uses the shared materials. 
 * Sharing project materials or using shared materials will also result 
 * in the reporting of every team member to the Provost Office for academic 
 * dishonesty. 
 */ 

package cs4347.jdbcProject.ecomm.dao;

import java.sql.Connection;
import java.sql.SQLException;

import cs4347.jdbcProject.ecomm.entity.Address;
import cs4347.jdbcProject.ecomm.util.DAOException;

/**
 * DAO that exclusively updates the ADDRESS table.
 */
public interface AddressDAO
{
	Address create(Connection connection, Address address, Long customerID) throws SQLException, DAOException;
	
	Address retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException;
	
	void deleteForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException;
}

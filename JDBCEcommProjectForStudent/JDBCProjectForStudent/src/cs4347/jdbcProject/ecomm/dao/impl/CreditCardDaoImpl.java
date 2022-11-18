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

package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import cs4347.jdbcProject.ecomm.dao.CreditCardDAO;
import cs4347.jdbcProject.ecomm.entity.CreditCard;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class CreditCardDaoImpl implements CreditCardDAO
{

	//General SQL statements
	private static final String insertSQL = 
			"INSERT INTO creditcard (name, cc_number, exp_date, security_code, CUSTOMER_id) VALUES (?, ?, ?, ?, ?);";
	
	private static final String selectSQL = 
			"SELECT * FROM creditcard WHERE CUSTOMER_id = ?;";
	
	private static final String deleteSQL = 
			"DELETE FROM creditcard WHERE CUSTOMER_id = ?;";
	
    @Override
    public CreditCard create(Connection connection, CreditCard creditCard, Long customerID) throws SQLException, DAOException
    {
    	if (customerID == null) 
            throw new DAOException("Credit card has no valid parent customer");
        
    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(insertSQL);
			ps.setString(1, creditCard.getName());
			ps.setString(2, creditCard.getCcNumber());
			ps.setString(3, creditCard.getExpDate());
			ps.setString(4, creditCard.getSecurityCode());
			ps.setLong(5, customerID);
			ps.executeUpdate();	
			
			return creditCard;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }

    @Override
    public CreditCard retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException
    {
    	if (customerID == null) 
			throw new DAOException("Attempting to retrieve non-existent ID");
		
    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(selectSQL);
			ps.setLong(1, customerID);
			ResultSet ccRS = ps.executeQuery();
			
			CreditCard cc = null;
			if(ccRS.next()) {
				cc = new CreditCard();
				cc.setName(ccRS.getString(1));
				cc.setCcNumber(ccRS.getString(2));
				cc.setExpDate(ccRS.getString(3));
				cc.setSecurityCode(ccRS.getString(4));
			}
			
			return cc;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }

    @Override
    public void deleteForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException
    {
    	if (customerID == null) 
			throw new DAOException("Attempting to retrieve non-existent ID");
		
    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(deleteSQL);
			ps.setLong(1, customerID);
			ps.executeUpdate();
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }
}

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
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import cs4347.jdbcProject.ecomm.dao.CustomerDAO;
import cs4347.jdbcProject.ecomm.entity.CreditCard;
import cs4347.jdbcProject.ecomm.entity.Customer;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class CustomerDaoImpl implements CustomerDAO
{
	
	//General SQL statements
    private static final String insertSQL = 
            "INSERT INTO customer (first_name, last_name, gender, dob, email) VALUES (?, ?, ?, ?, ?);";
    
    private static final String selectSQL = 
    		"SELECT * FROM customer WHERE id = ?;";
    
    private static final String selectDobSQL = 
    		"SELECT * FROM customer WHERE dob BETWEEN ? AND ?;"; //start date, end date
    
    private static final String selectZipSQL = 
    		"SELECT * FROM customer INNER JOIN address ON customer.id = address.CUSTOMER_id WHERE zipcode = ?;"; //SQL is the enemy
    
    private static final String updateSQL = 
    		"UPDATE customer SET id = ?, first_name = ?, last_name = ?, gender = ?, dob = ?, email = ? WHERE id = ?;";
    
    private static final String deleteSQL = 
    		"DELETE FROM customer WHERE id = ?;";

    @Override
    public Customer create(Connection connection, Customer customer) throws SQLException, DAOException
    {
        if (customer.getId() != null) 
            throw new DAOException("Trying to insert Customer with NON-NULL ID");
        
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, customer.getFirstName());
            ps.setString(2, customer.getLastName());
            ps.setString(3, String.valueOf(customer.getGender()));
            ps.setDate(4, customer.getDob());
            ps.setString(5, customer.getEmail());
            ps.executeUpdate();
            
            ResultSet keyRS = ps.getGeneratedKeys();
            keyRS.next();
            int lastKey = keyRS.getInt(1);
            customer.setId((long) lastKey);
            return customer;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    /*
     * return null if retrieving a non-existent ID
     * 
    */
    @Override
    public Customer retrieve(Connection connection, Long id) throws SQLException, DAOException
    {
    	if (id == null) 
			throw new DAOException("Attempting to retrieve non-existent ID");
		
    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(selectSQL);
			ps.setLong(1, id);
			ResultSet cusRS = ps.executeQuery();
			
			Customer cr = null;
			if(cusRS.next()) {
				cr = new Customer();
				cr.setId(cusRS.getLong(1));
				cr.setFirstName(cusRS.getString(2));
				cr.setLastName(cusRS.getString(3));
				cr.setGender(cusRS.getString(4).charAt(0));
				cr.setDob(cusRS.getDate(5));
				cr.setEmail(cusRS.getString(6));
			}

			return cr;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }

    @Override
    public int update(Connection connection, Customer customer) throws SQLException, DAOException
    {
    	if (customer.getId() == null) 
			throw new DAOException("Attempting to retrieve non-existent ID");
		
    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(updateSQL);
			ps.setLong(1, customer.getId());
			ps.setString(2, customer.getFirstName());
			ps.setString(3, customer.getLastName());
			ps.setString(4, String.valueOf(customer.getGender()));
			ps.setDate(5, customer.getDob());
			ps.setString(6, customer.getEmail());
			ps.setLong(7, customer.getId());
			
			return ps.executeUpdate();
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }

    @Override
    public int delete(Connection connection, Long id) throws SQLException, DAOException
    {
    	if (id == null) 
			throw new DAOException("Attempting to retrieve non-existent ID");
		
    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(deleteSQL);
			ps.setLong(1, id);
			
			return ps.executeUpdate();
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }

    @Override
    public List<Customer> retrieveByZipCode(Connection connection, String zipCode) throws SQLException, DAOException
    {
    	if (zipCode == null) 
			throw new DAOException("Attempting to retrieve non-existent ID");
		
    	PreparedStatement ps = null;
    	List<Customer> list = new LinkedList<>();
    	
		try {
			ps = connection.prepareStatement(selectZipSQL);
			ps.setString(1, zipCode);
			ResultSet cusRS = ps.executeQuery();
			
			while(cusRS.next()) {
				Customer cr = new Customer();
				
				cr.setId(cusRS.getLong(1));
				cr.setFirstName(cusRS.getString(2));
				cr.setLastName(cusRS.getString(3));
				cr.setGender(cusRS.getString(4).charAt(0)); //error here but can't figure it out for the life of me
				cr.setDob(cusRS.getDate(5));
				cr.setEmail(cusRS.getString(6));
				
				list.add(cr);
			}
			
			return list;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }

    //the usual retrieve function, except worse because you have a range of dates now
    // when looping, should be while.next(), add customer item to list.
    @Override
    public List<Customer> retrieveByDOB(Connection connection, Date startDate, Date endDate) throws SQLException, DAOException
    {
    	if (startDate.compareTo(endDate) > 0) {
			throw new DAOException("Attempting to retrieve non-existent ID");
		} //if the start date is after the end date, something's wrong probably
    	
    	PreparedStatement ps = null;
    	List<Customer> list = new LinkedList<>();
    	
		try {
			ps = connection.prepareStatement(selectDobSQL);
			ps.setDate(1, startDate);
			ps.setDate(2, endDate);
			ResultSet cusRS = ps.executeQuery();
			
			while(cusRS.next()) {
				Customer cr = new Customer();
				
				cr.setId(cusRS.getLong(1));
				cr.setFirstName(cusRS.getString(2));
				cr.setLastName(cusRS.getString(3));
				cr.setGender(cusRS.getString(4).charAt(0));
				cr.setDob(cusRS.getDate(5));
				cr.setEmail(cusRS.getString(6));
				
				list.add(cr);
			}
			
			return list;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }
}

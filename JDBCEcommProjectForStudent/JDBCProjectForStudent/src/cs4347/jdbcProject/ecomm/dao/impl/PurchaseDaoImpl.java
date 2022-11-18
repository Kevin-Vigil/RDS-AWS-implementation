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
import java.util.LinkedList;
import java.util.List;

import cs4347.jdbcProject.ecomm.dao.PurchaseDAO;
import cs4347.jdbcProject.ecomm.entity.Purchase;
import cs4347.jdbcProject.ecomm.services.PurchaseSummary;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class PurchaseDaoImpl implements PurchaseDAO
{
	
	//General SQL statements
	private static final String insertSQL = 
			"INSERT INTO Purchase (purchase_date, purchase_amt, CUSTOMER_id, PRODUCT_id) VALUES (?, ?, ?, ?);";
	
	private static final String retrieveSQL = 
			"SELECT * FROM Purchase WHERE id = ?;";
	
	private static final String updateSQL = 
			"UPDATE Purchase SET purchase_date = ?, purchase_amt = ?, CUSTOMER_id = ?, PRODUCT_id = ? WHERE id = ?;";
	
	private static final String deleteSQL = 
			"DELETE FROM Purchase WHERE id = ?;";
	
	private static final String retCusSQL = 
			"SELECT * FROM Purchase WHERE CUSTOMER_id = ?";
	
	private static final String retProSQL = 
			"SELECT * FROM Purchase WHERE PRODUCT_id = ?";
	
	private static final String retCusAmtSQL = 
			"SELECT purchase_amt FROM Purchase WHERE CUSTOMER_id = ?";
	
    @Override
    public Purchase create(Connection connection, Purchase purchase) throws SQLException, DAOException
    {
    	if (purchase.getId() != null) 
			throw new DAOException("Attempting to insert with NON-NULL ID");

    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			
			ps.setDate(1, purchase.getPurchaseDate());
			ps.setDouble(2, purchase.getPurchaseAmount());
			ps.setLong(3, purchase.getCustomerID());
			ps.setLong(4, purchase.getProductID());
			ps.executeUpdate();
			
			ResultSet keyRS = ps.getGeneratedKeys();
			keyRS.next();
			int lastKey = keyRS.getInt(1);
			purchase.setId((long) lastKey);
			
			return purchase;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }

    @Override
    public Purchase retrieve(Connection connection, Long id) throws SQLException, DAOException
    {
    	if (id == null) 
			throw new DAOException("Attempting to retrieve non-existent ID");
    	
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(retrieveSQL);
			ps.setLong(1, id);
			ResultSet prRS = ps.executeQuery();

			Purchase pur = null;
			if(prRS.next()) {
				pur = new Purchase();
				pur.setId(id);
				pur.setPurchaseDate(prRS.getDate(2));
				pur.setPurchaseAmount(prRS.getDouble(3));
				pur.setCustomerID(prRS.getLong(4));
				pur.setProductID(prRS.getLong(5));
			}

			return pur;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }

    @Override
    public int update(Connection connection, Purchase purchase) throws SQLException, DAOException
    {
    	if (purchase.getId() == null) 
			throw new DAOException("Attempting to update non-existent ID");
		
    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(updateSQL);
			ps.setDate(1, purchase.getPurchaseDate());
			ps.setDouble(2, purchase.getPurchaseAmount());
			ps.setLong(3, purchase.getCustomerID());
			ps.setLong(4, purchase.getProductID());
			ps.setLong(5, purchase.getId());
			
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
			throw new DAOException("Attempting to delete non-existent ID");
		
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
    public List<Purchase> retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException
    {
        if (customerID == null) 
			throw new DAOException("Attempting to retrieve non-existent ID");
		
        List<Purchase> list = new LinkedList<>();
    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(retCusSQL);
			ps.setLong(1, customerID);
			ResultSet psRS = ps.executeQuery();
			
			while(psRS.next()) {
				Purchase pr = new Purchase();
				
				pr.setId(psRS.getLong(1));
				pr.setPurchaseDate(psRS.getDate(2));
				pr.setPurchaseAmount(psRS.getDouble(3));
				pr.setCustomerID(psRS.getLong(4));
				pr.setProductID(psRS.getLong(5));
				
				list.add(pr);
			}
			
			return list;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }

    @Override
    public List<Purchase> retrieveForProductID(Connection connection, Long productID) throws SQLException, DAOException
    {
        if (productID == null) 
			throw new DAOException("Attempting to retrieve non-existent ID");
		
        List<Purchase> list = new LinkedList<>();
    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(retProSQL);
			ps.setLong(1, productID);
			ResultSet psRS = ps.executeQuery();
			
			while(psRS.next()) {
				Purchase pr = new Purchase();
				
				pr.setId(psRS.getLong(1));
				pr.setPurchaseDate(psRS.getDate(2));
				pr.setPurchaseAmount(psRS.getDouble(3));
				pr.setCustomerID(psRS.getLong(4));
				pr.setProductID(psRS.getLong(5));
				
				list.add(pr);
			}
			
			return list;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }

    @Override
    public PurchaseSummary retrievePurchaseSummary(Connection connection, Long customerID) throws SQLException, DAOException
    {
        if (customerID == null) 
			throw new DAOException("Attempting to retrieve non-existent ID");
		
        PurchaseSummary purSum = new PurchaseSummary();
        purSum.minPurchase = Integer.MAX_VALUE;
        purSum.maxPurchase = Integer.MIN_VALUE;
        purSum.avgPurchase = 0f;
        int counter = 0;
        
    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(retCusAmtSQL);
			ps.setLong(1, customerID);
			ResultSet psRS = ps.executeQuery();
			
			while(psRS.next()) {
				counter++;
				float temp = psRS.getFloat(1);
				purSum.avgPurchase += temp;
				if(temp > purSum.maxPurchase)
					purSum.maxPurchase = temp;
				if(temp < purSum.minPurchase)
					purSum.minPurchase = temp;
			}
			if(counter == 0)
				return null;
			purSum.avgPurchase /= counter;
			
			return purSum;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }
}

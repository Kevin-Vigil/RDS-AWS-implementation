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

import javax.sql.DataSource;

import cs4347.jdbcProject.ecomm.dao.ProductDAO;
import cs4347.jdbcProject.ecomm.entity.Product;
import cs4347.jdbcProject.ecomm.entity.Purchase;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class ProductDaoImpl implements ProductDAO
{

	//General SQL statements
	private static final String insertSQL = 
			"INSERT INTO Product (prod_category, prod_desc, prod_name, prod_upc) VALUES (?, ?, ?, ?);";
	
	private static final String selectSQL = 
			"SELECT * FROM Product WHERE id = ?";
	
	private static final String updateSQL = 
			"UPDATE Product SET prod_name = ?, prod_desc = ?, prod_category = ?, prod_upc = ? WHERE id = ?;";
	
	private static final String deleteSQL = 
			"DELETE FROM Product WHERE id = ?;";
	
	private static final String catSQL = 
			"SELECT * FROM Product WHERE prod_category = ?";
	
	private static final String upcSQL = 
			"SELECT * FROM Product WHERE prod_UPC = ?";
			
	/**
	 * The create method must throw a DAOException if the 
	 * given Product has a non-null ID. The create method must 
	 * return the same Product with the ID attribute set to the
	 * value set by the application's auto-increment primary key column. 
	 * @throws DAOException if the given Product has a non-null id.
	 */
    @Override
    public Product create(Connection connection, Product product) throws SQLException, DAOException
    {
    	if (product.getId() != null)
			throw new DAOException("Trying to insert product with NON-NULL ID");

		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			
			ps.setInt(1, product.getProdCategory());
			ps.setString(2, product.getProdDescription());
			ps.setString(3, product.getProdName());
			ps.setString(4, product.getProdUPC());
			ps.executeUpdate();

			ResultSet keyRS = ps.getGeneratedKeys();
			keyRS.next();
			int lastKey = keyRS.getInt(1);
			product.setId((long) lastKey);
			
			return product;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }
    
    /**
	 * The update method must throw DAOException if the provided 
	 * ID is null. 
	 */
    @Override
    public Product retrieve(Connection connection, Long id) throws SQLException, DAOException
    {
    	if (id == null)
			throw new DAOException("Trying to retrieve products with NULL ID");
    	
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(selectSQL);
			ps.setLong(1, id);
			ResultSet prRS = ps.executeQuery();
		
			Product product = null;
			if(prRS.next()) {
				product = new Product();
				product.setId(id);
				product.setProdName(prRS.getString(2));
				product.setProdDescription(prRS.getString(3));
				product.setProdCategory(prRS.getInt(4));
				product.setProdUPC(prRS.getString(5));
			}
			
			return product;
		}
		finally {
			if (ps != null && !ps.isClosed())
				ps.close();
		}
	}

	/**
	 * The update method must throw DAOException if the provided 
	 * Product has a NULL id. 
	 */
    @Override
    public int update(Connection connection, Product product) throws SQLException, DAOException
    {
    	if (product.getId() == null)
			throw new DAOException("Attempting to update non-existent ID");
    	
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(updateSQL);
			ps.setString(1, product.getProdName());
			ps.setString(2, product.getProdDescription());
			ps.setInt(3, product.getProdCategory());
			ps.setString(4, product.getProdUPC());
			ps.setLong(5, product.getId());
			
			return ps.executeUpdate();
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }

	/**
	 * The update method must throw DAOException if the provided 
	 * ID is null. 
	 */
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

    /**
	 * Retrieve products in the given product category
	 */
    @Override
    public List<Product> retrieveByCategory(Connection connection, int category) throws SQLException, DAOException
    {
        if (category < 0)
			throw new DAOException("Category is invalid");
        
        List<Product> list = new LinkedList<>();
    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(catSQL);
			ps.setLong(1, category);
			ResultSet prRS = ps.executeQuery();
			
			while(prRS.next()) {
				Product pr = new Product();
				
				pr.setId(prRS.getLong(1));
				pr.setProdName(prRS.getString(2));
				pr.setProdDescription(prRS.getString(3));
				pr.setProdCategory(prRS.getInt(4));
				pr.setProdUPC(prRS.getString(5));
				
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

    /**
	 * Retrieve the product with the given UPC. UPC is unique across all product. 
	 */
    @Override
    public Product retrieveByUPC(Connection connection, String upc) throws SQLException, DAOException
    {
        if (upc == null)
			throw new DAOException("Trying to retrieve products with NULL UPC");
        
    	PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(upcSQL);
			ps.setString(1, upc);
			ResultSet prRS = ps.executeQuery();
			
			Product pr = null;
			if(prRS.next()) {
				pr = new Product();
				pr.setId(prRS.getLong(1));
				pr.setProdName(prRS.getString(2));
				pr.setProdDescription(prRS.getString(3));
				pr.setProdCategory(prRS.getInt(4));
				pr.setProdUPC(prRS.getString(5));
			}
				
			return pr;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
    }
}

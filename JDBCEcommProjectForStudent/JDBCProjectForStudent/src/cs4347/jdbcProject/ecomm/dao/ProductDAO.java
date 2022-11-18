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
import java.util.List;

import cs4347.jdbcProject.ecomm.entity.Product;
import cs4347.jdbcProject.ecomm.util.DAOException;

/**
 * DAO that exclusively updates the PRODUCT table. 
 */
public interface ProductDAO
{
	/**
	 * The create method must throw a DAOException if the 
	 * given Product has a non-null ID. The create method must 
	 * return the same Product with the ID attribute set to the
	 * value set by the application's auto-increment primary key column. 
	 * @throws DAOException if the given Product has a non-null id.
	 */
	Product create(Connection connection, Product product) throws SQLException, DAOException;
	
	/**
	 * The update method must throw DAOException if the provided 
	 * ID is null. 
	 */
	Product retrieve(Connection connection, Long id) throws SQLException, DAOException;
	
	/**
	 * The update method must throw DAOException if the provided 
	 * Product has a NULL id. 
	 */
	int update(Connection connection, Product product) throws SQLException, DAOException;
	
	/**
	 * The update method must throw DAOException if the provided 
	 * ID is null. 
	 */
	int delete(Connection connection, Long id) throws SQLException, DAOException;
	
	/**
	 * Retrieve products in the given product category
	 */
	List<Product> retrieveByCategory(Connection connection, int category) throws SQLException, DAOException;

	/**
	 * Retrieve the product with the given UPC. UPC is unique across all product. 
	 */
	Product retrieveByUPC(Connection connection, String upc) throws SQLException, DAOException;

}

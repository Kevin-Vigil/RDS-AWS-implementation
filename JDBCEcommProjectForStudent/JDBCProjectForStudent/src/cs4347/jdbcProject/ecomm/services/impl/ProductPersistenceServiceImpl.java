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

package cs4347.jdbcProject.ecomm.services.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcProject.ecomm.dao.ProductDAO;
import cs4347.jdbcProject.ecomm.dao.PurchaseDAO;
import cs4347.jdbcProject.ecomm.dao.impl.ProductDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.PurchaseDaoImpl;
import cs4347.jdbcProject.ecomm.entity.Product;
import cs4347.jdbcProject.ecomm.entity.Purchase;
import cs4347.jdbcProject.ecomm.services.ProductPersistenceService;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class ProductPersistenceServiceImpl implements ProductPersistenceService
{
	@Override
    public Product create(Product product) throws SQLException, DAOException
    {
		if(product.getId() != null) {
			throw new DAOException("Purchase must have NULL ID");
		}
		ProductDAO productDAO = new ProductDaoImpl();
		Product pur;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            pur = productDAO.create(connection, product);
            connection.commit();
            
            return pur;
        }
        catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public Product retrieve(Long id) throws SQLException, DAOException
    {
    	if(id == null) {
			throw new DAOException("ID must be a NON-NULL ID");
		}
    	
    	ProductDAO productDAO = new ProductDaoImpl();
		Product prod;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            prod = productDAO.retrieve(connection, id);
            connection.commit();
            return prod;
        }
        catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public int update(Product product) throws SQLException, DAOException
    {
    	if(product.getId() == null) {
			throw new DAOException("Product must have a NON-NULL ID");
		}
    	
    	ProductDAO ProductDAO = new ProductDaoImpl();
    	int result;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            result = ProductDAO.update(connection, product);
            connection.commit();
            return result;
        }
        catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public int delete(Long id) throws SQLException, DAOException
    {
    	if(id == null) {
			throw new DAOException("ID must be a NON-NULL ID");
		}
    	
    	ProductDAO productDAO = new ProductDaoImpl();
    	int result;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            result = productDAO.delete(connection, id);
            connection.commit();
            return result;
        }
        catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public Product retrieveByUPC(String upc) throws SQLException, DAOException
    {
    	if(upc == null) {
			throw new DAOException("UPC must be a NON-NULL String");
		}
    	
    	ProductDAO productDAO = new ProductDaoImpl();
    	Product prod = new Product();
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            prod = productDAO.retrieveByUPC(connection, upc);
            connection.commit();
            return prod;
        }
        catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public List<Product> retrieveByCategory(int category) throws SQLException, DAOException
    {
    	if(category <0) {
			throw new DAOException("UPC must be a NON-NULL String");
		}
    	
    	ProductDAO productDAO = new ProductDaoImpl();
    	List<Product> prod;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            prod = productDAO.retrieveByCategory(connection, category);
            connection.commit();
            return prod;
        }
        catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    private DataSource dataSource;

	public ProductPersistenceServiceImpl(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}

}

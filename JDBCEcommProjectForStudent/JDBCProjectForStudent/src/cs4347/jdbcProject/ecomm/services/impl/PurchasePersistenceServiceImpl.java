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

import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcProject.ecomm.dao.AddressDAO;
import cs4347.jdbcProject.ecomm.dao.CreditCardDAO;
import cs4347.jdbcProject.ecomm.dao.CustomerDAO;
import cs4347.jdbcProject.ecomm.dao.PurchaseDAO;
import cs4347.jdbcProject.ecomm.dao.impl.AddressDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.CreditCardDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.CustomerDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.PurchaseDaoImpl;
import cs4347.jdbcProject.ecomm.entity.Address;
import cs4347.jdbcProject.ecomm.entity.CreditCard;
import cs4347.jdbcProject.ecomm.entity.Customer;
import cs4347.jdbcProject.ecomm.entity.Purchase;
import cs4347.jdbcProject.ecomm.services.PurchasePersistenceService;
import cs4347.jdbcProject.ecomm.services.PurchaseSummary;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class PurchasePersistenceServiceImpl implements PurchasePersistenceService
{
	@Override
    public Purchase create(Purchase purchase) throws SQLException, DAOException
    {
		if(purchase.getId() != null) {
			throw new DAOException("Purchase must have NULL ID");
		}
		PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
		Purchase pur;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            pur = purchaseDAO.create(connection, purchase);
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
    public Purchase retrieve(Long id) throws SQLException, DAOException
    {
    	if(id == null) {
			throw new DAOException("ID must be a NON-NULL ID");
		}
    	
    	PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
		Purchase pur;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            pur = purchaseDAO.retrieve(connection, id);
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
    public int update(Purchase purchase) throws SQLException, DAOException
    {
    	if(purchase.getId() == null) {
			throw new DAOException("Purchase must have a NON-NULL ID");
		}
    	
    	PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
    	int result;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            result = purchaseDAO.update(connection, purchase);
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
    	
    	PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
    	int result;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            result = purchaseDAO.delete(connection, id);
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
    public List<Purchase> retrieveForCustomerID(Long customerID) throws SQLException, DAOException
    {
    	if(customerID == null) {
			throw new DAOException("customerID must be a NON-NULL ID");
		}
    	
    	PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
    	List<Purchase> list;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            list = purchaseDAO.retrieveForCustomerID(connection, customerID);
            connection.commit();
            
            return list;
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
    public PurchaseSummary retrievePurchaseSummary(Long customerID) throws SQLException, DAOException
    {
    	if(customerID == null) {
			throw new DAOException("customerID must be a NON-NULL ID");
		}
    	
    	PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
    	PurchaseSummary summary;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            summary = purchaseDAO.retrievePurchaseSummary(connection, customerID);
            connection.commit();
            
            return summary;
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
    public List<Purchase> retrieveForProductID(Long productID) throws SQLException, DAOException
    {
    	if(productID == null) {
			throw new DAOException("productID must be a NON-NULL ID");
		}
    	
    	PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
    	List<Purchase> list;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            list = purchaseDAO.retrieveForProductID(connection, productID);
            connection.commit();
            
            return list;
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

	public PurchasePersistenceServiceImpl(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}

}

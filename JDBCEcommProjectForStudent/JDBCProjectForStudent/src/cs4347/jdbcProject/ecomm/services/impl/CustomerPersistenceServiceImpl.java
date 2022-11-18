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
import java.sql.Date;
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
import cs4347.jdbcProject.ecomm.services.CustomerPersistenceService;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class CustomerPersistenceServiceImpl implements CustomerPersistenceService
{
    /**
     * This method provided as an example of transaction support across multiple inserts.
     * 
     * Persists a new Customer instance by inserting new Customer, Address, 
     * and CreditCard instances. Notice the transactional nature of this 
     * method which includes turning off autocommit at the start of the 
     * process, and rolling back the transaction if an exception 
     * is caught. 
     */
    @Override
    public Customer create(Customer customer) throws SQLException, DAOException
    {
        CustomerDAO customerDAO = new CustomerDaoImpl();
        AddressDAO addressDAO = new AddressDaoImpl();
        CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            Customer cust = customerDAO.create(connection, customer);
            Long custID = cust.getId();

            if (cust.getAddress() == null) {
                throw new DAOException("Customers must include an Address instance.");
            }
            Address address = cust.getAddress();
            addressDAO.create(connection, address, custID);

            if (cust.getCreditCard() == null) {
                throw new DAOException("Customers must include an CreditCard instance.");
            }
            CreditCard creditCard = cust.getCreditCard();
            creditCardDAO.create(connection, creditCard, custID);

            connection.commit();
            return cust;
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
    public Customer retrieve(Long id) throws SQLException, DAOException
    {
    	if (id == null) {
			throw new DAOException("Attempting to retrieve non-existent ID");
    	}
    	
    	CustomerDAO customerDAO = new CustomerDaoImpl();
        AddressDAO addressDAO = new AddressDaoImpl();
        CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

        Connection connection = dataSource.getConnection();
        
        try {
        	connection.setAutoCommit(false);  // Starts new Transaction on Connection
            Customer cust = customerDAO.retrieve(connection, id);
            
            cust.setAddress(addressDAO.retrieveForCustomerID(connection, id));
            cust.setCreditCard(creditCardDAO.retrieveForCustomerID(connection, id));
            
            connection.commit();
            
            return cust;
        }
        catch(Exception ex){
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
    public int update(Customer customer) throws SQLException, DAOException
    {
    	if(customer.getId() == null) 
			throw new DAOException("Customer must have a NON-NULL ID");
    	
    	CustomerDAO customerDAO = new CustomerDaoImpl();
    	AddressDAO addressDAO = new AddressDaoImpl();
        CreditCardDAO creditCardDAO = new CreditCardDaoImpl();
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            
            int rows = customerDAO.update(connection, customer);
            
            Long custID = customer.getId();
            addressDAO.deleteForCustomerID(connection, custID);
            creditCardDAO.deleteForCustomerID(connection, custID);
            
            addressDAO.create(connection, customer.getAddress(), custID);
            creditCardDAO.create(connection, customer.getCreditCard(), custID);
            
            connection.commit();
            
            return rows;
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
    	if(id == null) 
			throw new DAOException("ID must be a NON-NULL ID");
		
    	CustomerDAO customerDAO = new CustomerDaoImpl();
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            int rows = customerDAO.delete(connection, id);
            connection.commit();
            
            return rows;
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
    public List<Customer> retrieveByZipCode(String zipCode) throws SQLException, DAOException
    {
    	if(zipCode == null) 
			throw new DAOException("zipCode must be a NON-NULL ID");
		
    	CustomerDAO customerDAO = new CustomerDaoImpl();
    	AddressDAO addressDAO = new AddressDaoImpl();
        CreditCardDAO creditCardDAO = new CreditCardDaoImpl();
    	List<Customer> list;
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);  // Starts new Transaction on Connection
            list = customerDAO.retrieveByZipCode(connection, zipCode);
            
            for(Customer cust: list) {
            	cust.setAddress(addressDAO.retrieveForCustomerID(connection, cust.getId()));
            	cust.setCreditCard(creditCardDAO.retrieveForCustomerID(connection, cust.getId()));
            }
            
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
    public List<Customer> retrieveByDOB(Date startDate, Date endDate) throws SQLException, DAOException
    {
    	if (startDate.compareTo(endDate) > 0) {
			throw new DAOException("Start date is later than end date");
    	}
    	
    	CustomerDAO customerDAO = new CustomerDaoImpl();
        AddressDAO addressDAO = new AddressDaoImpl();
        CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

        Connection connection = dataSource.getConnection();
        
        try {
        	connection.setAutoCommit(false);  // Starts new Transaction on Connection
            List<Customer> custLis = customerDAO.retrieveByDOB(connection, startDate, endDate);
            
            for(Customer cust: custLis) {
            	cust.setAddress(addressDAO.retrieveForCustomerID(connection, cust.getId()));
            	cust.setCreditCard(creditCardDAO.retrieveForCustomerID(connection, cust.getId()));
            }
            
            connection.commit();
            
            return custLis;
        }
        catch(Exception ex){
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

	public CustomerPersistenceServiceImpl(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}
	
}

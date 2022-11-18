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

package cs4347.jdbcProject.ecomm.entity;

import java.sql.Date;

public class Purchase
{
	private Long id;
	private Long customerID;
	private Long productID;
	private Date purchaseDate;
	private double purchaseAmount;

	public Long getCustomerID()
	{
		return customerID;
	}

	public void setCustomerID(Long customerID)
	{
		this.customerID = customerID;
	}

	public Date getPurchaseDate()
	{
		return purchaseDate;
	}

	public void setPurchaseDate(Date purchaseDate)
	{
		this.purchaseDate = purchaseDate;
	}

	public double getPurchaseAmount()
	{
		return purchaseAmount;
	}

	public void setPurchaseAmount(double purchaseAmount)
	{
		this.purchaseAmount = purchaseAmount;
	}

	public Long getProductID()
	{
		return productID;
	}

	public void setProductID(Long productID)
	{
		this.productID = productID;
	}

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

}

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

public class CreditCard
{
	private String name;
	private String ccNumber;
	private String expDate;
	private String securityCode;

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public String getCcNumber()
	{
		return ccNumber;
	}

	public void setCcNumber(String ccNumber)
	{
		this.ccNumber = ccNumber;
	}

	public String getExpDate()
	{
		return expDate;
	}

	public void setExpDate(String expDate)
	{
		this.expDate = expDate;
	}

	public String getSecurityCode()
	{
		return securityCode;
	}

	public void setSecurityCode(String securityCode)
	{
		this.securityCode = securityCode;
	}

}

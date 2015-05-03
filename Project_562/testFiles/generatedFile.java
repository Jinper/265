import java.sql.*;
import java.util.*;

/*
 * For query:
 * select product, month, year, sum(X.quantity)/sum(Y.quantity)
 * from Sales
 * group by product,month,year ; X,Y
 * such that X.product=product and X.month=month and X.year=year,
 *           Y.product=product and Y.year=year
 */
class mf_Structure0
{
	//Grouping Atts
	ArrayList<String> product = new ArrayList<String>();
	ArrayList<Integer> month = new ArrayList<Integer>();
	ArrayList<Integer> year = new ArrayList<Integer>();
	//Grouping Vars
	ArrayList<Integer> sum_1_quant = new ArrayList<Integer>();
	ArrayList<Integer> sum_2_quant = new ArrayList<Integer>();
	
	mf_Structure0(){}
	int getSize()
	{
		return product.size();
	}
	
	void output()
	{
		System.out.println("product month year sum_1_quant sum_2_quant");
		for(int i=0;i<getSize();i++)
		{
			System.out.printf("%-7s %5d %4d %11d %11d",product.get(i),month.get(i),year.get(i),sum_1_quant.get(i),sum_2_quant.get(i));
			System.out.println();
		}
	}
}

public class generatedFile 
{
	public static void main(String[] args) 
	{
		try
		{
			Class.forName("org.postgresql.Driver");
			System.out.println("Loading Driver Successfully!");
		}catch(Exception e)
		{
			System.out.println("Error Loading MySQL Drive!");
			e.printStackTrace();
		}
		//Connect to database
		try
		{
			Connection connect=DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb","postgres","zuoyouzuo");
			System.out.println("Success Connect Dadabase!");
			
			Statement stmt=connect.createStatement();
			ResultSet rs=stmt.executeQuery("select * from salesman");
			
			mf_Structure0 mf = new mf_Structure0();
			
			
			//第一遍扫描
			while(rs.next())
			{
				boolean found = false;
				String product = rs.getString("prod");
				int year = rs.getInt("year");
				int month = rs.getInt("month");
				
				if(mf.getSize()==0)
				{
					mf.product.add(product);
					mf.month.add(month);
					mf.year.add(year);
					mf.sum_1_quant.add(0);
					mf.sum_2_quant.add(0);
				}
				else
				{
					for(int i=0;i<mf.getSize();i++)
					{
						//存在,更新
						if(product.equals(mf.product.get(i)) && year==mf.year.get(i) && month==mf.month.get(i))
						{
							found = true;
							break;
							//X0无条件
						}
					}
					if(!found)
					{
						mf.product.add(product);
						mf.month.add(month);
						mf.year.add(year);
						mf.sum_1_quant.add(0);
						mf.sum_2_quant.add(0);
					}
				}							
			}
			
			//第二次扫描
			rs=stmt.executeQuery("select * from salesman");
			while(rs.next())
			{
				boolean found = false;
				String product = rs.getString("prod");
				int year = rs.getInt("year");
				int month = rs.getInt("month");
				int quant = rs.getInt("quant");
				
				for(int i=0;i<mf.getSize();i++)
				{
					//存在,更新
					if(product.equals(mf.product.get(i)) && year==mf.year.get(i) && month==mf.month.get(i))
					{
						found = true;
						mf.sum_1_quant.set(i, quant+mf.sum_1_quant.get(i));
						break;
						//X0无条件
					}
				}
				if(!found)
				{
					System.out.println("Error occours!");
					System.exit(-1);
				}
			}
			
			//第三次扫描	
			rs=stmt.executeQuery("select * from salesman");
			while(rs.next())
			{
				boolean found = false;
				String product = rs.getString("prod");
				int year = rs.getInt("year");
				int month = rs.getInt("month");
				int quant = rs.getInt("quant");
				
				for(int i=0;i<mf.getSize();i++)
				{
					//存在,更新
					if(product.equals(mf.product.get(i)) && year==mf.year.get(i))
					{
						found = true;
						mf.sum_2_quant.set(i, quant+mf.sum_2_quant.get(i));
						break;
						//X0无条件
					}
				}
				if(!found)
				{
					System.out.println("Error occours!");
					System.exit(-1);
				}
			}
			
			
			mf.output();
			
			
		}catch(Exception e)
		{
			System.out.println("Error Getting Data!");
			e.printStackTrace();
		}
	}
}

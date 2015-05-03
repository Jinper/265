import java.sql.*;
import java.util.*;

/*
 * For query:
 * select product, month, avg(X.quantity),avg(Y.quantity)
	from Sales
	where year=‘‘1997’’
	group by product, month; X , Y
	such that X.product=product and X.month<month,
	          Y.product=product and Y.month>month
 */

class mf_Structure2
{
	//Grouping Atts
	ArrayList<String> product = new ArrayList<String>();
	ArrayList<Integer> month = new ArrayList<Integer>();
	//Grouping Vars
	ArrayList<Integer> sum_1_quant = new ArrayList<Integer>();
	ArrayList<Integer> count_1_quant = new ArrayList<Integer>();
	
	ArrayList<Integer> sum_2_quant = new ArrayList<Integer>();
	ArrayList<Integer> count_2_quant = new ArrayList<Integer>();
	
	ArrayList<Integer> avg_1_quant = new ArrayList<Integer>();
	ArrayList<Integer> avg_2_quant = new ArrayList<Integer>();
	
	int getSize()
	{
		return product.size();
	}

	mf_Structure2(){}
	
	void output()
	{
		System.out.printf("%-7s %5d %11d %13d %11d %13d",product,month,sum_1_quant,count_1_quant,
				sum_2_quant,count_2_quant);
		System.out.println();
	}
}

public class generatedFile2 {
	public static void main(String[] args) {
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
			ResultSet rs=stmt.executeQuery("select * from sales");
			
			mf_Structure2 mf = new mf_Structure2();
			
			//第一次扫描
			while(rs.next())
			{
				boolean found = false;
				String product = rs.getString("prod");
				int month = rs.getInt("month");
				int year = rs.getInt("year");
				
				if(mf.getSize()==0 && year==1991)
				{
					mf.product.add(product);
					mf.month.add(month);
					mf.sum_1_quant.add(0);
					mf.sum_2_quant.add(0);
					mf.count_1_quant.add(0);
					mf.count_2_quant.add(0);
					mf.avg_1_quant.add(0);
					mf.avg_2_quant.add(0);
				}
				else
				{
					//存在，更新
					for(int i =0;i<mf.getSize();i++)
					{
						if(mf.product.get(i).equals(product) && mf.month.get(i)==month && year==1991)//有条件
						{
							found = true;
							break;
						}
					}
					if((!found) && year==1991)
					{
						mf.product.add(product);
						mf.month.add(month);
						mf.sum_1_quant.add(0);
						mf.sum_2_quant.add(0);
						mf.count_1_quant.add(0);
						mf.count_2_quant.add(0);
						mf.avg_1_quant.add(0);
						mf.avg_2_quant.add(0);
					}
				}
			}
			
			//第二次扫描
			rs = stmt.executeQuery("select * from sales");
			while(rs.next())
			{
				boolean found = false;
				String product = rs.getString("prod");
				int month = rs.getInt("month");
				int year = rs.getInt("year");
				int quant = rs.getInt("quant");//第一次扫描不需要
				
				//存在，更新
				for(int i=0;i<mf.getSize();i++)
				{
					if(mf.product.get(i).equals(product) && mf.month.get(i)<month && year==1991)
					{
						found = true;
						mf.count_1_quant.set(i, mf.count_1_quant.get(i)+1);
						mf.sum_1_quant.set(i, mf.sum_1_quant.get(i)+quant);
						break;
					}
				}	
			}
			for(int i=0;i<mf.getSize();i++)
			{
				if(mf.count_1_quant.get(i)==0)
					mf.avg_1_quant.set(i, 0);
				else
					mf.avg_1_quant.set(i, mf.sum_1_quant.get(i)/mf.count_1_quant.get(i));
				
			}
			
			//第三次扫描
			rs = stmt.executeQuery("select * from sales");
			while(rs.next())
			{
				boolean found = false;
				String product = rs.getString("prod");
				int month = rs.getInt("month");
				int year = rs.getInt("year");
				int quant = rs.getInt("quant");//第一次扫描不需要
				
				//存在，更新
				for(int i=0;i<mf.getSize();i++)
				{
					if(mf.product.get(i).equals(product) && mf.month.get(i)>month && year==1991)
					{
						found = true;
						mf.count_2_quant.set(i, mf.count_2_quant.get(i)+1);
						mf.sum_2_quant.set(i, mf.sum_2_quant.get(i)+quant);
						break;
					}
				}
			}
			for(int i=0;i<mf.getSize();i++)
			{
				if(mf.count_2_quant.get(i)==0)
					mf.avg_2_quant.set(i, 0);
				else
					mf.avg_2_quant.set(i, mf.sum_2_quant.get(i)/mf.count_2_quant.get(i));		
			}
			mf.output();
		}catch(Exception e)
		{
			System.out.println("Getting data error!");
			e.printStackTrace();
		}
	}

}

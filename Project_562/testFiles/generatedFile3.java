import java.sql.*;
import java.util.*;


/*
 * select cust, sum(x.quant), sum(y.quant), sum(z.quant) 
 * from sales 
 * group by cust: x, y, z 
 * such that x.state = ‘NY’ and y.state = ‘NJ’ and z.state = ‘CT’ 
 * having sum(x.quant) > 2 * sum(y.quant) or avg(x.quant) > avg(z.quant); 
 */
class mf_Structure3
{
	ArrayList<String> customer = new ArrayList<String>();
	
	ArrayList<Integer> sum_1_quant = new ArrayList<Integer>();
	ArrayList<Integer> sum_2_quant = new ArrayList<Integer>();
	ArrayList<Integer> sum_3_quant = new ArrayList<Integer>();
	
	ArrayList<Integer> count_1_quant = new ArrayList<Integer>();
	ArrayList<Integer> count_2_quant = new ArrayList<Integer>();
	ArrayList<Integer> count_3_quant = new ArrayList<Integer>();
	
	mf_Structure3(){}
	
	int getSize()
	{
		return customer.size();
	}
	
	void output()
	{
		
		
		System.out.println("customer sum_1_quant sum_2_quant avg_1_quant avg_3_quant");
		
		for(int i =0;i<getSize();i++)
		{
			int avg_1_quant = 0;
			int avg_3_quant = 0;
			if(!(count_1_quant.get(i)==0))
			{
				avg_1_quant = sum_1_quant.get(i)/count_1_quant.get(i);
			}
			
			if(!(count_3_quant.get(i)==0))
			{
				avg_3_quant = sum_3_quant.get(i)/count_3_quant.get(i);
			}
			
			System.out.printf("%-8s %11d %11d %11d %11d",customer.get(i),sum_1_quant.get(i),sum_2_quant.get(i),
					avg_1_quant,avg_3_quant);
			System.out.println();
		}
	}
}

public class generatedFile3 {

	public static void main(String[] args) {
		try
		{
			Class.forName("org.postgresql.Driver");
			System.out.println("Successful Loading Driver!");
		}catch(Exception e)
		{
			System.out.println("Failed Loading Driver!");
			e.printStackTrace();
		}
		
		try
		{
			Connection connect=DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb","postgres","zuoyouzuo");
			System.out.println("Success Connect Dadabase!");
			
			Statement stmt=connect.createStatement();
			ResultSet rs=stmt.executeQuery("select * from salesman");
			
			mf_Structure3 mf = new mf_Structure3();
			//第一次扫描
			while(rs.next())
			{
				boolean found = false;
				String customer = rs.getString("cust");
				//添加第一条记录
				if(mf.getSize()==0)
				{
					mf.customer.add(customer);
					mf.count_1_quant.add(0);
					mf.count_2_quant.add(0);
					mf.count_3_quant.add(0);
					mf.sum_1_quant.add(0);
					mf.sum_2_quant.add(0);
					mf.sum_3_quant.add(0);
				}
				else
				{
					//存在更新
					for(int i=0;i<mf.getSize();i++)
					{
						if(customer.equals(mf.customer.get(i)))
						{
							found = true;
							break;
						}
					}
					//不在添加
					if(!found)
					{
						mf.customer.add(customer);
						mf.count_1_quant.add(0);
						mf.count_2_quant.add(0);
						mf.count_3_quant.add(0);
						mf.sum_1_quant.add(0);
						mf.sum_2_quant.add(0);
						mf.sum_3_quant.add(0);
					}
				}
			}
			
			//第二,三，四次 扫描
			rs = stmt.executeQuery("select * from salesman");
			while(rs.next())
			{
				String customer = rs.getString("cust");
				int quant = rs.getInt("quant");
				String state = rs.getString("state");
				for(int i=0;i<mf.getSize();i++)
				{
					if(customer.equals(mf.customer.get(i)))
					{
						if(state.equals("NY"))
						{
							mf.sum_1_quant.set(i, mf.sum_1_quant.get(i)+quant);
							mf.count_1_quant.set(i, mf.count_1_quant.get(i)+1);
							break;
						}
						else if(state.equals("NJ"))
						{
							mf.sum_2_quant.set(i, mf.sum_2_quant.get(i)+quant);
							mf.count_2_quant.set(i, mf.count_2_quant.get(i)+1);
							break;
						}
						else if(state.equals("CT"))
						{
							mf.sum_3_quant.set(i, mf.sum_3_quant.get(i)+quant);
							mf.count_3_quant.set(i, mf.count_3_quant.get(i)+1);
							break;
						}
					}
				}
			}
			
			
			//第一次修改
			mf_Structure3 mf2 = new mf_Structure3();
			for(int i=0;i<mf.getSize();i++)
			{
				if((mf.sum_1_quant.get(i)>(2*mf.sum_2_quant.get(i))) || (mf.count_1_quant.get(i)>mf.count_3_quant.get(i)))
				{
					mf2.customer.add(mf.customer.get(i));
					mf2.count_1_quant.add(mf.count_1_quant.get(i));
					mf2.count_2_quant.add(mf.count_2_quant.get(i));
					mf2.count_3_quant.add(mf.count_3_quant.get(i));
					mf2.sum_1_quant.add(mf.sum_1_quant.get(i));
					mf2.sum_2_quant.add(mf.sum_2_quant.get(i));
					mf2.sum_3_quant.add(mf.sum_3_quant.get(i));
				}
			}
			
			
			mf.output();
		}catch(Exception e)
		{
			System.out.println("Failed Extracting Data!");
			e.printStackTrace();
		}

	}

}

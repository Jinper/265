import java.sql.*;
import java.util.*;
class Tuple {
	int quant;
	String state;
	int month;
	int year;
	int day;
	String prod;
	String cust;
}

class MF_Structure {
	ArrayList<String> prod= new ArrayList<String>();
	ArrayList<Integer> quant= new ArrayList<Integer>();
	ArrayList<Integer> count_1_prod= new ArrayList<Integer>();
	ArrayList<Integer> count_2_prod= new ArrayList<Integer>();
	MF_Structure(){}
	int getSize() {
		return prod.size();
	}
}

public class Query6_Output {
	public static void main(String args[]) {
		try {
			Class.forName("org.postgresql.Driver");
			System.out.println("Loading Driver Successfully!");
		}catch(Exception e) {
			System.out.println("Failed Extracting Data!");
			e.printStackTrace();
		}
		try {
			Connection connect=DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb","postgres","zuoyouzuo");
			System.out.println("Success Connect Dadabase!");
			Statement stmt=connect.createStatement();
			MF_Structure mf = new MF_Structure();
			ResultSet rs=stmt.executeQuery("select * from sales");
			while(rs.next()) {
				boolean found = false;
				Tuple tuple = new Tuple();
				tuple.quant = rs.getInt("quant");
				tuple.state = rs.getString("state");
				tuple.month = rs.getInt("month");
				tuple.year = rs.getInt("year");
				tuple.day = rs.getInt("day");
				tuple.prod = rs.getString("prod");
				tuple.cust = rs.getString("cust");
				if(mf.getSize()==0&&true){
					mf.prod.add(tuple.prod);
					mf.quant.add(tuple.quant);
					mf.count_1_prod.add(0);
					mf.count_2_prod.add(0);
				}
				else{
					for(int i=0;i<mf.getSize();i++){
						if(mf.prod.get(i).equals(tuple.prod)&&mf.quant.get(i).equals(tuple.quant)&&true){
							found = true;
							break;
						}
					}
					if(!found&&true){
					mf.prod.add(tuple.prod);
					mf.quant.add(tuple.quant);
					mf.count_1_prod.add(0);
					mf.count_2_prod.add(0);
					}
				}
			}

//-----------------------scan 1 to n------------------------
			rs = stmt.executeQuery("select * from sales");
			while(rs.next()){
				Tuple tuple = new Tuple();
				tuple.quant = rs.getInt("quant");
				tuple.state = rs.getString("state");
				tuple.month = rs.getInt("month");
				tuple.year = rs.getInt("year");
				tuple.day = rs.getInt("day");
				tuple.prod = rs.getString("prod");
				tuple.cust = rs.getString("cust");
				for(int i=0;i<mf.getSize();i++){
					if(mf.prod.get(i).equals(tuple.prod)&&true){
						mf.count_1_prod.set(i,mf.count_1_prod.get(i)+1);
					}
				}
			}
			rs = stmt.executeQuery("select * from sales");
			while(rs.next()){
				Tuple tuple = new Tuple();
				tuple.quant = rs.getInt("quant");
				tuple.state = rs.getString("state");
				tuple.month = rs.getInt("month");
				tuple.year = rs.getInt("year");
				tuple.day = rs.getInt("day");
				tuple.prod = rs.getString("prod");
				tuple.cust = rs.getString("cust");
				for(int i=0;i<mf.getSize();i++){
					if(mf.prod.get(i).equals(tuple.prod)&&tuple.quant>mf.quant.get(i)&&true){
						mf.count_2_prod.set(i,mf.count_2_prod.get(i)+1);
					}
				}
			}
			MF_Structure mfs = new MF_Structure();
			for(int i=0;i<mf.getSize();i++) {
				if(mf.count_2_prod.get(i)==mf.count_1_prod.get(i)/2){
					mfs.prod.add(mf.prod.get(i));
					mfs.quant.add(mf.quant.get(i));
					mfs.count_1_prod.add(mf.count_1_prod.get(i));
					mfs.count_2_prod.add(mf.count_2_prod.get(i));
				}
			}
			System.out.println("prod     quant ");
			for(int i=0;i<mfs.getSize();i++){
				float x=0;
				System.out.printf("%-8s %5d ",mfs.prod.get(i),mfs.quant.get(i));
				System.out.println();
			}
			try{
				connect.close(); stmt.close(); rs.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}catch(Exception e) {
			System.out.println("Oops!Error Occurs...");
			e.printStackTrace();
		}
	}
}

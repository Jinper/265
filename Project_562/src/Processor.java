import java.sql.*;
import java.util.*;
import java.io.*;

class Query{
	String type;
	String whereClause;
	int numOfGroupVar;
	String havingClause;
	
	ArrayList<String> selectAtt = new ArrayList<String>();
	ArrayList<String> groupAtt = new ArrayList<String>();
	ArrayList<String> groupVar = new ArrayList<String>();
	ArrayList<ArrayList<String>> aggregate = new ArrayList<ArrayList<String>>();

	Query(){}
}

public class Processor {
	public static void main(String[] args) {
		try
		{
			//----------------------------------------读数据库------------------------------------
			HashMap<String,String> dbStructure = new HashMap();
			try 
			{
				Class.forName("org.postgresql.Driver");     
				System.out.println("Success loading Driver!");
			} catch(Exception exception)
			{
				System.out.println("Fail loading Driver!");
				exception.printStackTrace();
			}
			
			try
			{
				Connection connect=DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb","postgres","zuoyouzuo");
				System.out.println("Success Connect Dadabase!");
				
				Statement stmt=connect.createStatement();
				ResultSet rs=stmt.executeQuery("select data_type,column_name from information_schema.columns where table_name='sales'");
				
				while(rs.next()){
					String temp;
					if(rs.getString("data_type").equals("character varying") || rs.getString("data_type").equals("character"))
						temp = "String";
					else
						temp = "int";
					dbStructure.put(rs.getString("column_name"), temp);
				}
				
				try
				{
					rs.close();
					stmt.close();
					connect.close();
					System.out.println("Success close connection!");
				}catch(Exception e)
				{
					e.printStackTrace();
				}
			}catch(Exception e)
			{
				System.out.println("Failed Extracting Data!");
				e.printStackTrace();
			}
			
			//----------------------------------------读文件--------------------------------------
			
			System.out.print("Enter the direction of your file please: ");
			Scanner scan = new Scanner(System.in);
			String fileDir = scan.nextLine();
			
			File file = new File(fileDir);
			Scanner sc = new Scanner(file);
			
			String type  = sc.nextLine();
			String [] projection = sc.nextLine().split(",");
			String where = sc.nextLine();
			String [] groupAtt = sc.nextLine().split(",");
			int numOfGroupVar = Integer.parseInt(sc.nextLine());
			String [] aggregates = sc.nextLine().split(",");
			String [] groupVar = sc.nextLine().split(",");
			String having = sc.nextLine();
			
			//------------------------------存储参数---------------------------
			Query query = new Query();
			//检查类型  ----------需要添加异常
			if(!(type.equals("EMF")||type.equals("MF")))
			{
				System.out.println("Unvalid type of query!");
				System.exit(-1);
			}else
				query.type = type;
			//存储selectAtt 和 groupAtt
			for(String s:projection)
				query.selectAtt.add(s);
			for(String s:groupAtt)
				query.groupAtt.add(s);
			for(String s:groupVar)
				query.groupVar.add(s);
			for(String s:aggregates)
			{
				String[] temp = s.split("_");
				ArrayList<String> list = new ArrayList<String>();
				for(String ss:temp)
				{
					list.add(ss);
				}
				query.aggregate.add(list);
			}
			
			//存储where 和 number 和 having
			query.whereClause = where;
			query.numOfGroupVar = numOfGroupVar;
			query.havingClause = having;
			
			
			//--------------------------------------写入------------------------------------------------
			String fileTitle = fileDir.replace(".txt", "_Output");
			String outFileName = fileTitle + ".java";
			File outFile = new File(outFileName);
			
			//检查文件是否已存在
			if(!outFile.exists())
			{
				//创建文件不成功
				if(!outFile.createNewFile())
					System.out.println("Failed to create file!");
				//创建文件成功
				else
				{
					PrintWriter pw = new PrintWriter(outFile);
					
					pw.println("import java.sql.*;\n" + "import java.util.*;");   //写入import 语句
					
					//-----------------------------写入定义tuple语句------------------------------
					pw.println("class Tuple {");
					
					for(Map.Entry<String, String> entry:dbStructure.entrySet())
					{
						pw.println("\t" + entry.getValue() + " " + entry.getKey() + ";");
					}
					
					pw.println("}");
					pw.println();
					
					//-----------------------------写入定义mf语句---------------------------------
					pw.println("class MF_Structure {");
					
					for(String s:query.groupAtt)
					{
						if(dbStructure.get(s).equals("String"))
							pw.println("\tArrayList<String> " + s + "= new ArrayList<String>();");
						else
							pw.println("\tArrayList<Integer> " + s + "= new ArrayList<Integer>();");
					}
					//
					for(ArrayList<String> list:query.aggregate)
					{
						String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
						if(s.contains("avg"))
						{
							String sum = s.replace("avg", "sum");
							String count = s.replace("avg", "count");
							pw.println("\tArrayList<Integer> " + sum +"= new ArrayList<Integer>();");
							pw.println("\tArrayList<Integer> " + count +"= new ArrayList<Integer>();");
							pw.println("\tArrayList<Integer> " + s +"= new ArrayList<Integer>();");
						}
						else
							pw.println("\tArrayList<Integer> " + s +"= new ArrayList<Integer>();");
					}
					
					pw.println("\tMF_Structure(){}");
					pw.println("\tint getSize() {");
					pw.println("\t\treturn "+query.groupAtt.get(0)+".size();");
					pw.println("\t}");
						
					pw.println("}");
					pw.println();
					//---------------------------------------------写入主函数----------------------------------------------
					String temple = outFileName.replace("put.java", "put");
					String[] temple2 = temple.split("\\\\");
					pw.println("public class "+temple2[temple2.length-1]+" {");
					pw.println("\tpublic static void main(String args[]) {");
					//----------------------写入加载驱动----------------------
					pw.println("\t\ttry {");
					pw.println("\t\t\tClass.forName(\"org.postgresql.Driver\");");
					pw.println("\t\t\tSystem.out.println(\"Loading Driver Successfully!\");");
					pw.println("\t\t}catch(Exception e) {");
					pw.println("\t\t\tSystem.out.println(\"Failed Extracting Data!\");");
					pw.println("\t\t\te.printStackTrace();");
					pw.println("\t\t}");
					
					//----------------------写入连接数据库---------------------
					pw.println("\t\ttry {");
					pw.println("\t\t\tConnection connect=DriverManager.getConnection(\"jdbc:postgresql://localhost:5432/testdb\",\"postgres\",\"zuoyouzuo\");");
					pw.println("\t\t\tSystem.out.println(\"Success Connect Dadabase!\");");
					pw.println("\t\t\tStatement stmt=connect.createStatement();");
					pw.println("\t\t\tMF_Structure mf = new MF_Structure();");
					
					//----------------------写入执行语句----------------------
					//--------------第一次扫描-------------
					pw.println("\t\t\tResultSet rs=stmt.executeQuery(\"select * from sales\");");
					pw.println("\t\t\twhile(rs.next()) {");
					pw.println("\t\t\t\tboolean found = false;");
					pw.println("\t\t\t\tTuple tuple = new Tuple();");
					//接收数据
					for(Map.Entry<String, String> entry:dbStructure.entrySet())
					{
						if(entry.getValue().equals("String"))
							pw.println("\t\t\t\ttuple."+entry.getKey()+" = rs.getString(\""+entry.getKey()+"\");");
						else if(entry.getValue().equals("int"))
							pw.println("\t\t\t\ttuple."+entry.getKey()+" = rs.getInt(\""+entry.getKey()+"\");");
					}
					
					//检查是否为空
					
					pw.println("\t\t\t\tif(mf.getSize()==0&&"+query.whereClause+"){");
					
					for(String s:query.groupAtt)
					{
						pw.println("\t\t\t\t\tmf."+s+".add(tuple."+s+");");
					}
					
					for(ArrayList<String> list:query.aggregate)
					{
						String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
						if(list.get(1).equals("0"))
						{
							if(list.get(0).equals("sum")||list.get(0).equals("max")||list.get(0).equals("min"))
							{
								pw.println("\t\t\t\t\tmf."+s+".add("+"tuple."+list.get(2)+");");
							}
							else if(list.get(0).equals("count"))
								pw.println("\t\t\t\t\tmf."+s+".add(1);");
							else if(list.get(0).equals("avg"))
							{
								pw.println("\t\t\t\t\tmf.sum_0_"+list.get(2)+".add("+"tuple."+list.get(2)+");");
								pw.println("\t\t\t\t\tmf.count_0_"+list.get(2)+".add(1);");
								pw.println("\t\t\t\t\tmf."+s+".add(0);");
							}
						}
						else
						{
							if(list.get(0).equals("avg"))
							{
								pw.println("\t\t\t\t\tmf.sum_"+list.get(1)+"_"+list.get(2)+".add(0);");
								pw.println("\t\t\t\t\tmf.count_"+list.get(1)+"_"+list.get(2)+".add(0);");
								pw.println("\t\t\t\t\tmf."+s+".add(0);");
							}
							else
								pw.println("\t\t\t\t\tmf."+s+".add(0);");
						}
					}
					pw.println("\t\t\t\t}"); //if结束
					
					pw.println("\t\t\t\telse{"); //else，非空遍历
					pw.println("\t\t\t\t\tfor(int i=0;i<mf.getSize();i++){");
					
					//---------------------if(mf.product.get(i).equals(product) && mf.month.get(i)==month && year==1991)----
					pw.print("\t\t\t\t\t\tif(");
					for(String s:query.groupAtt)
					{
						pw.print("mf."+s+".get(i).equals(tuple."+s+")&&");
					}
					pw.print(query.whereClause+"){\n");
					pw.println("\t\t\t\t\t\t\tfound = true;");
					for(ArrayList<String> list:query.aggregate)
					{
						if(list.get(1).equals("0"))
						{
							String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
							if(list.get(0).equals("sum"))
								pw.println("\t\t\t\t\t\t\tmf."+s+".set(i,mf."+s+".get(i)+tuple."+list.get(2)+");");
							else if(list.get(0).equals("count"))
								pw.println("\t\t\t\t\t\t\tmf."+s+".set(i,mf."+s+".get(i)+1);");
							else if(list.get(0).equals("avg"))
							{
								String sum = s.replace("avg", "sum");
								String count = s.replace("avg", "count");
								pw.println("\t\t\t\t\t\t\tmf."+sum+".set(i,mf."+sum+".get(i)+tuple."+list.get(2)+");");
								pw.println("\t\t\t\t\t\t\tmf."+count+".set(i,mf."+count+".get(i)+1);");
							}
							else if(list.get(0).equals("max"))
							{
								pw.println("\t\t\t\t\t\t\tif(tuple."+list.get(2)+")>mf."+s+".get(i)");
								pw.println("\t\t\t\t\t\t\t\tmf."+s+".set(i,tuple."+list.get(2)+");");
							}
							else if(list.get(0).equals("min"))
							{
								pw.println("\t\t\t\t\t\t\tif(tuple."+list.get(2)+")<mf."+s+".get(i)");
								pw.println("\t\t\t\t\t\t\t\tmf."+s+".set(i,tuple."+list.get(2)+");");
							}
						}
					}
					pw.println("\t\t\t\t\t\t\tbreak;");
					pw.println("\t\t\t\t\t\t}");//if 结束
					pw.println("\t\t\t\t\t}");//for 结束
					
					//------------------------if((!found) && year==1991)----------------------
					pw.println("\t\t\t\t\tif(!found&&"+query.whereClause+"){");
					for(String s:query.groupAtt)
					{
						pw.println("\t\t\t\t\tmf."+s+".add(tuple."+s+");");
					}
					
					for(ArrayList<String> list:query.aggregate)
					{
						String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
						if(list.get(1).equals("0"))
						{
							if(list.get(0).equals("sum")||list.get(0).equals("max")||list.get(0).equals("min"))
							{
								pw.println("\t\t\t\t\tmf."+s+".add("+"tuple."+list.get(2)+");");
							}
							else if(list.get(0).equals("count"))
								pw.println("\t\t\t\t\tmf."+s+".add(1);");
							else if(list.get(0).equals("avg"))
							{
								pw.println("\t\t\t\t\tmf.sum_0_"+list.get(2)+".add("+"tuple."+list.get(2)+");");
								pw.println("\t\t\t\t\tmf.count_0_"+list.get(2)+".add(1);");
								pw.println("\t\t\t\t\tmf."+s+".add(0);");
							}
						}
						else
						{
							if(list.get(0).equals("avg"))
							{
								pw.println("\t\t\t\t\tmf.sum_"+list.get(1)+"_"+list.get(2)+".add(0);");
								pw.println("\t\t\t\t\tmf.count_"+list.get(1)+"_"+list.get(2)+".add(0);");
								pw.println("\t\t\t\t\tmf."+s+".add(0);");
							}
							else
								pw.println("\t\t\t\t\tmf."+s+".add(0);");
						}
					}
					pw.println("\t\t\t\t\t}");
					pw.println("\t\t\t\t}");//else结束
					pw.println("\t\t\t}");//while 结束
					for(ArrayList<String> list:query.aggregate)
					{
						
						if(list.get(0).equals("avg")&&list.get(1).equals("0"))
						{
							String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
							String sum = s.replace("avg", "sum");
							String count = s.replace("avg", "count");
							pw.println("\t\t\tfor(int i=0;i<mf.getSize();i++){");
							pw.println("\t\t\t\tif(mf."+count+".get(i)==0)");
							pw.println("\t\t\t\t\tmf."+s+".set(i,0);");
							pw.println("\t\t\t\telse");
							pw.println("\t\t\t\t\tmf."+s+".set(i,mf."+sum+".get(i)/mf."+count+".get(i));");
							pw.println("\t\t\t}");
						}
					}
					
					pw.println();
					pw.println("//-----------------------scan 1 to n------------------------");
					//-----------------------后续扫描------------------------
					for(int n=1;n<=query.numOfGroupVar;n++)
					{
						pw.println("\t\t\trs = stmt.executeQuery(\"select * from sales\");");
						pw.println("\t\t\twhile(rs.next()){");
						pw.println("\t\t\t\tTuple tuple = new Tuple();");
						for(Map.Entry<String, String> entry:dbStructure.entrySet())
						{
							if(entry.getValue().equals("String"))
								pw.println("\t\t\t\ttuple."+entry.getKey()+" = rs.getString(\""+entry.getKey()+"\");");
							else if(entry.getValue().equals("int"))
								pw.println("\t\t\t\ttuple."+entry.getKey()+" = rs.getInt(\""+entry.getKey()+"\");");
						}
						pw.println("\t\t\t\tfor(int i=0;i<mf.getSize();i++){");
						
						//区分mf和emf
						if(query.type.equals("EMF"))
							pw.println("\t\t\t\t\tif("+query.groupVar.get(n-1)+"&&"+query.whereClause+"){");
						
						else if(query.type.equals("MF"))
						{
							pw.print("\t\t\t\t\tif(");
							for(String s:query.groupAtt)
							{
								pw.print("tuple."+s+".equals(mf."+s+".get(i))"+"&&");
							}
							pw.print(query.groupVar.get(n-1)+query.whereClause+"){");
						}
						for(ArrayList<String> list:query.aggregate)
						{
							if(list.get(1).equals(String.valueOf(n)))
							{
								String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
								if(list.get(0).equals("sum"))
									pw.println("\t\t\t\t\t\tmf."+s+".set(i,mf."+s+".get(i)+tuple."+list.get(2)+");");
								else if(list.get(0).equals("count"))
									pw.println("\t\t\t\t\t\tmf."+s+".set(i,mf."+s+".get(i)+1);");
								else if(list.get(0).equals("avg"))
								{
									String sum = s.replace("avg", "sum");
									String count = s.replace("avg", "count");
									pw.println("\t\t\t\t\t\tmf."+sum+".set(i,mf."+sum+".get(i)+tuple."+list.get(2)+");");
									pw.println("\t\t\t\t\t\tmf."+count+".set(i,mf."+count+".get(i)+1);");
								}
								else if(list.get(0).equals("max"))
								{
									pw.println("\t\t\t\t\t\tif(tuple."+list.get(2)+")>mf."+s+".get(i)");
									pw.println("\t\t\t\t\t\t\tmf."+s+".set(i,tuple."+list.get(2)+");");
								}
								else if(list.get(0).equals("min"))
								{
									pw.println("\t\t\t\t\t\tif(tuple."+list.get(2)+")<mf."+s+".get(i)");
									pw.println("\t\t\t\t\t\t\tmf."+s+".set(i,tuple."+list.get(2)+");");
								}
							}
						}
						
						
						
						pw.println("\t\t\t\t\t}");
						
						pw.println("\t\t\t\t}");//for 结束
						pw.println("\t\t\t}");//while 结束
						
						for(ArrayList<String> list:query.aggregate)
						{
							
							if(list.get(0).equals("avg")&&list.get(1).equals(String.valueOf(n)))
							{
								String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
								String sum = s.replace("avg", "sum");
								String count = s.replace("avg", "count");
								pw.println("\t\t\tfor(int i=0;i<mf.getSize();i++){");
								pw.println("\t\t\t\tif(mf."+count+".get(i)==0)");
								pw.println("\t\t\t\t\tmf."+s+".set(i,0);");
								pw.println("\t\t\t\telse");
								pw.println("\t\t\t\t\tmf."+s+".set(i,mf."+sum+".get(i)/mf."+count+".get(i));");
								pw.println("\t\t\t}");
							}
						}
					}//n次扫描结束
					
					//------------------------------having部分-------------------------
					if(query.havingClause.equals("null"))
						pw.println("\t\t\tMF_Structure mfs = mf;");
					else
					{
						pw.println("\t\t\tMF_Structure mfs = new MF_Structure();");
						pw.println("\t\t\tfor(int i=0;i<mf.getSize();i++) {");
						pw.println("\t\t\t\tif("+query.havingClause+"){");
						for(String s:query.groupAtt)
						{
							pw.println("\t\t\t\t\tmfs."+s+".add(mf."+s+".get(i));");
						}
						for(ArrayList<String> list:query.aggregate)
						{
							String s = list.get(0)+"_"+list.get(1)+"_"+list.get(2);
							pw.println("\t\t\t\t\tmfs."+s+".add(mf."+s+".get(i));");
						}
						
						pw.println("\t\t\t\t}");
						pw.println("\t\t\t}");
					}
					
					
					//------------------------------写入显示结果部分------------------------
					pw.print("\t\t\tSystem.out.println(\"");
					for(String string:query.selectAtt)
					{
						String[] temp = string.split("\\.");
						String s = temp[1];
						if(s.equals("prod"))
							pw.print(s+"     ");
						else if(s.equals("cust"))
							pw.print(s+"     ");
						else if(string.contains("/"))
						{
							
							String[] t = string.split("/");
							String[] t1 = t[0].split("\\.");
							String[] t2 = t[1].split("\\.");
							String combo = t1[1]+"/"+t2[1];
							
							pw.print(combo+" ");
						}
						else
							pw.print(s+" ");
					}
					pw.print("\");\n");
					pw.println("\t\t\tfor(int i=0;i<mfs.getSize();i++){");
					pw.println("\t\t\t\tfloat x=0;");
					for(String s:query.selectAtt)
					{
						if(s.contains("/"))
						{
							String[] temp = s.split("/");
							pw.println("\t\t\t\tif("+temp[temp.length-1]+"!=0)");
							pw.println("\t\t\t\t\tx=(float)"+s+";");
							
						}
					}
					
					pw.print("\t\t\t\tSystem.out.printf(\"");
					for(String string:query.selectAtt)
					{
						String [] str = string.split("\\.");
						String s = str[1];
						int size = s.length();
						if(s.equals("prod")||s.equals("cust"))
							size = 8;
						
						String align;
						String c;
						
						if(dbStructure.containsKey(s))
						{
							if(dbStructure.get(s).equals("String"))
							{
								align = "-";
								c = "s";
							}
							else 
							{
								align = "";
								c = "d";
							}
						}
						else
						{
							if(string.contains("/"))
							{
								c = "f";
								size = 0;
								String[] temp = string.split("/");
								for(String t:temp)
								{
									String[] tempTemp = t.split("\\.");
									size = size + tempTemp[1].length();
								}
								size++;
							}
							else
								c = "d";
							align = "";
						}
						
						pw.print("%"+align+String.valueOf(size)+c+" ");
					}
					pw.print("\"");
					for(String s:query.selectAtt)
					{
						if(s.contains("/"))
							pw.print(",x");
						else
							pw.print(","+s);
					}
					pw.print(");\n");
					pw.println("\t\t\t\tSystem.out.println();");
					
					pw.println("\t\t\t}");
					
					pw.println("\t\t\ttry{");
					pw.println("\t\t\t\tconnect.close(); stmt.close(); rs.close();");
					pw.println("\t\t\t}catch(Exception e){");
					pw.println("\t\t\t\te.printStackTrace();");
					pw.println("\t\t\t}");
					
					//---------------------------------------------------------------------------------------
					pw.println("\t\t}catch(Exception e) {");
					pw.println("\t\t\tSystem.out.println(\"Oops!Error Occurs...\");");
					pw.println("\t\t\te.printStackTrace();");
					pw.println("\t\t}");
					
					
					pw.println("\t}");
					pw.println("}");
					
					
					
					
					pw.flush();
				}
			}
		}//输入。输出检查--------------------需要更多
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}


}

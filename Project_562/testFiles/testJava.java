import java.io.*;
import java.lang.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
public class testJava {
	public static void main(String args[])
	{
		String s = "mf.prod.get(i)";
		String[] temp = s.split("\\.");
		String ss = temp[1];
			System.out.println(ss);
		
		
	}

}

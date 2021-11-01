import java.util.*;
import java.lang.*;
import java.io.*;


public class CourseProject {

	public static void main(String[] args) throws IOException
	{
		Scanner input = new Scanner(System.in);  

		System.out.println("Enter Text");

		String str= input.nextLine();

		System.out.print("You have entered: "+str);  

		File file = new File("inputFile.txt");
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));          
		writer.write(str);    
		writer.close(); 

	}
}
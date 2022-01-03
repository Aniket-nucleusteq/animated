package variables;

import java.util.Scanner;

public class Statvar2 {
	static double a;	//static variable
	static double b;	//static variable
	
public static void main(String[] args) {
	Scanner sc=new Scanner(System.in);
	System.out.println("Enter the number:");
	a=sc.nextDouble();
	System.out.println("Enter the Power value you want to get:");
	b=sc.nextDouble();
	System.out.println("The "+a+" rest to power "+b+" is: "+Math.pow(a,b));
	sc.close();
}
}

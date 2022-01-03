package variables;

import java.util.Scanner;

public class Var1 {
int a;		//Instance variable
int b;		//Instance variable	
int c;		//Instance variable
int add()
{	
	int s=a+b;		//Local variable
	return s;
}
public static void main(String[] args) {
	Var1 obj=new Var1();
	Scanner sc=new Scanner(System.in);
	System.out.println("Enter two numbers to add:");
	obj.a=sc.nextInt();
	obj.b=sc.nextInt();
	obj.c=obj.add();
	System.out.println("The sum of two numbers is:"+obj.c);
	sc.close();
}
}

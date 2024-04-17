package com.incometax.calculator;

import org.apache.hadoop.hive.ql.exec.UDF;

//creating a UDF that decides the incometax based on the employmenttype

public class IncomeTaxCalculator extends UDF{
	public String evaluate(String employmentType) {
		//declaring a variable to hold the income tax percentage
		String incomeTax = " ";  
		//To check the employmenttype and return the income tax percent to be paid
		if(employmentType.equals("SelfEmplo")) {
			incomeTax = "5%";
		}
		else if(employmentType.equals("Private")) {
			incomeTax =  "10%";
		}
		else {
			incomeTax = "15%";
		}
		return incomeTax;
	}
}

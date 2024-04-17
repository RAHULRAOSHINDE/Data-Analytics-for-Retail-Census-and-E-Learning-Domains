package com.course.validation;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

//Temporarily accumulate data which has InstructorId values as NULL
//Apply the UDF on the data where InstructorId is NULL


public class InstructorValidator extends EvalFunc<String>
{
	@Override
	public String exec(Tuple InputData) throws IOException {
		if(InputData == null || InputData.size()==0) {
			return null;
		}
		String OfferingMode = (String) InputData.get(0);
		String InstructorId ="";
		if(OfferingMode.equals("Instructor led_WebPage")){
			InstructorId="Offline Instructor_WP";
		}
		else if(OfferingMode.equals("Instructor led_Virtual Class")) {
			InstructorId="Offline Instructor_VC";
		}
		else if(OfferingMode.equals("Instructor led_Classroom")) {
			InstructorId="Online Instructor - CR";
		}
		else if(OfferingMode.equals("Online")) {
			InstructorId="HUB Instructor - DCS";
		}
		else if(OfferingMode.equals("Blended Learning")){
			InstructorId="Offline Instructor_BL";
		}
		else if(OfferingMode.equals("Instructor led_Mobile VC")) {
			InstructorId="Faculty";
		}
		return InstructorId;
	}

}

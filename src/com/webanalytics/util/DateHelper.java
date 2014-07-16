package com.webanalytics.util;

import java.util.Calendar;
import java.util.Date;

public class DateHelper {


	
	public static String getDateStartInHumanReadable(Long time){
		String str = "";
		Date date = new Date(time);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		str = str + cal.get(Calendar.YEAR);
		if( cal.get(Calendar.MONTH) < 10 ){
			str = str +"0"+ cal.get(Calendar.MONTH);
		}else{
			str = str +"0"+ cal.get(Calendar.MONTH);
		}
		if( cal.get(Calendar.DATE) < 10 ){
			str = str +"0"+ cal.get(Calendar.DATE);
		}else{
			str = str +"0"+ cal.get(Calendar.DATE);
		}
		return str;
	}

}

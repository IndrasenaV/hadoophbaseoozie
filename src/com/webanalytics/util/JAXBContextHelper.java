package com.webanalytics.util;

import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.PropertyException;
import javax.xml.bind.Unmarshaller;

public class JAXBContextHelper {

	public static String objectToXml(Object request)  {
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(request.getClass());
			Marshaller marshaller = jaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			StringWriter xmlWriter = new StringWriter();
			marshaller.marshal(request, xmlWriter);
			String xmlInput = xmlWriter.toString();
			return xmlInput;
		} catch (PropertyException e) {
			e.printStackTrace();
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static Object xmlToObject(String xmlResponse,
			Class cl)  {
		try {
			JAXBContext jaxbResponseContext = JAXBContext
					.newInstance(cl);
			Unmarshaller unmarshaller = jaxbResponseContext.createUnmarshaller();
			return unmarshaller.unmarshal(new StringReader(xmlResponse));
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}

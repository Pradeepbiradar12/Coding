package com.acdc.entity.service.impl;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import static org.neo4j.driver.Values.parameters;


public class LoadDatetoNeo4j implements AutoCloseable {
    @SuppressWarnings("unchecked")
    public static void main(String[] args)  throws FileNotFoundException {
        //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();

        try (FileReader reader = new FileReader("/home/pradeep/Downloads/convertcsv Final.json"))
        {
            //Read JSON file
            Object obj = jsonParser.parse(reader);

            JSONArray employeeList = (JSONArray) obj;

            Iterator<JSONObject> iterator = employeeList.iterator();

            try ( LoadDatetoNeo4j greeter = new LoadDatetoNeo4j( "bolt://54.80.251.50:7687", "neo4j", "i-07a2183189fade84a" ) )
            {
                while (iterator.hasNext()) {
                    JSONObject object = iterator.next();
                    greeter.printGreeting(object.get("A").toString(), "+" + object.get("B").toString(),  Integer.parseInt(object.get("D").toString()),  Integer.parseInt(object.get("C").toString()), Integer.parseInt(object.get("E").toString()), UUID.randomUUID().toString());
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            //Iterate over employee array
            //employeeList.forEach( emp -> parseEmployeeObject( (JSONObject) emp ) );

        } catch (FileNotFoundException e) {

        } catch (IOException e) {
        } catch (ParseException e) {
        }


    }
    private final Driver driver;

    public LoadDatetoNeo4j( String uri, String user, String password )
    {
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

    public void printGreeting( final String country,String code, int phoneNumberSizeMax, int phoneNumberSizeMin, int phoneNumberLength, String osid)
    {
        try ( Session session = driver.session() )
        {
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    Result result = tx.run( "CREATE (a:CountryCode) " +
                                    "SET a.country = $country " +
                            "SET a.phoneNumberSizeMax = $phoneNumberSizeMax " +
                            "SET a.phoneNumberSizeMin = $phoneNumberSizeMin " +
                            "SET a.phoneNumberLength = $phoneNumberLength " +
                            "SET a.osid = $osid " +
                            "SET a.code = $code " +
                            "RETURN a.country + ', from node ' + id(a) ", parameters( "country", country
                    , "code", code,"phoneNumberSizeMax", phoneNumberSizeMax,"phoneNumberSizeMin", phoneNumberSizeMin,"phoneNumberLength", phoneNumberLength,"osid", osid));
                    return result.single().get( 0 ).asString();
                }


            } );
            System.out.println( greeting );
        }
    }
    
}

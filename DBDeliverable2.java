import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TGroupByItemList;
import gudusoft.gsqlparser.nodes.TOrderByItemList;
import gudusoft.gsqlparser.nodes.TResultColumnList;
//import gudusoft.gsqlparser.nodes.TTableElementList;
import gudusoft.gsqlparser.nodes.TTableList;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Scanner;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.Iterator;
public class DBDeliverable2 {
	static String filePath = "/home/shrikant/IIIT/Mtech2Sem/DB/DB Deliverable 2/";
	private static String dataPath=null;
	//private static HashMap<String,Integer> tableNames = new HashMap<String,Integer>();
	private HashMap<String,LinkedHashMap<String,String>> tableWithColumnNames = new HashMap<String,LinkedHashMap<String,String>>(); 
	private final TGSqlParser sqlParser = new TGSqlParser(EDbVendor.dbvoracle);
	private int parserStatus;
	private int elementNumber;
	private int maxTableCount = 5000; /*keeps the limit of number of tables in  main memory for tableNames & column Names validation*/
	/**
	 * isValidColumn(TResultColumnList columnList,TTableList stmtTables) will update the printCoumnSet HashSet with corresponding colunms to print
	 */
	private LinkedHashSet<String> printColumnSet = new LinkedHashSet<String>();
	private HashMap<String,Integer> groupBySet = new HashMap<String,Integer>();
	private HashMap<String,Integer> havingMap = new HashMap<String,Integer>(); 
	private String inputQueryString;
	private StringBuilder distinctAttributeSet = new StringBuilder();
	
	/*******************DB3 Deliverables******************************/
	DBDeliverable3 db3Obj = new DBDeliverable3();
	private LinkedHashSet<Integer> columnNumSet = new LinkedHashSet<Integer>();
	private LinkedHashMap<Integer,String> orderByColNumMap = new LinkedHashMap<Integer,String>();
	private HashMap<Integer,String> columnDataTypeMap = new HashMap<Integer,String>();
	private HashMap<String,Integer> columnNumberMap = new HashMap<String,Integer>();
	private LinkedHashMap<Integer,Vector<WhereClauseCondition>> whereConditionMap = new LinkedHashMap<Integer,Vector<WhereClauseCondition>>();
	/*****************************************************************/
	
	void initiaLizeTableName() throws IOException{
		BufferedReader configFileReader = new BufferedReader(new FileReader(filePath));
		for(int i=0;i<2;i++)/*read first three lines*/
			configFileReader.readLine();
		dataPath = configFileReader.readLine();
		dataPath = dataPath.substring(dataPath.indexOf(' ')+1, dataPath.length());
		String tempString = configFileReader.readLine();
		StringBuilder tableName = new StringBuilder();
		boolean tableStartFlag=true,tableNameFlag=true;
		LinkedHashMap<String,String> tempColumnMap = null ;
		while(tempString !=null ){
			
			if(tempString.equals("BEGIN")){
				tableStartFlag = true;
				tableNameFlag = true;
			}
			else if (tableStartFlag && tempString.equals("END")){
				tableWithColumnNames.put(tableName.toString(), tempColumnMap);
				tableStartFlag = false;
			}
			else if(tableNameFlag)/*Add only table name to HashMap*/{
				tableName.setLength(0);
				tableName.append(tempString.trim());
				tempColumnMap = new LinkedHashMap<String,String>();
				tableNameFlag = false;
			}	
			else/*Meta data of the current table*/
			{
				String []column = {"",""};
				column = tempString.split(",");
				column[0] = column[0].trim();//removeSpace(column[0]).toString().trim();
				column[1] = column[1].trim();//removeSpace(column[1]).toString().trim();
				if(!column[0].equals("PRIMARY_KEY"))
				tempColumnMap.put(column[0],column[1].toUpperCase());
			}
			tempString = configFileReader.readLine();
		}
		/*
		Iterator<String> it = tableWithColumnNames.keySet().iterator();
		String tempTableName = null;
		while(it.hasNext()){
			tempTableName = it.next();
			System.out.println("Table:"+tempTableName);
			Iterator<String> it1 = tableWithColumnNames.get(tempTableName).keySet().iterator();
			LinkedHashMap<String,String> h1= tableWithColumnNames.get(tempTableName); 
			while(it1.hasNext()){
				String col = it1.next();
				System.out.print(col+":"+h1.get(col)+",");
			}
			System.out.println("");
		}*/
		configFileReader.close();
	}
	/**
	 * main method 
	 * @param args
	 */
	public static void main(String []args) throws IOException{
		
		filePath = args[0];
		DBDeliverable2 dbObj = new DBDeliverable2();
		dbObj.initiaLizeTableName();
		DBDeliverable3 tempObj = new DBDeliverable3();
		//System.exit(0);
		tempObj.createIndex(dbObj.tableWithColumnNames.keySet(),dataPath);
	//	System.exit(0);
		//Scanner inputScanner = new Scanner(new File(args[1]));
		Scanner inputScanner = new Scanner(System.in);
		//System.out.println("Enter the number of test cases:");
		inputScanner.useDelimiter("\n");
		int tests = inputScanner.nextInt();
		
		for(int i=0;i<tests;i++){
		//while(inputScanner.hasNext()){
			//System.out.println("Enter the query ending with ';' : ");
			String queryString = inputScanner.next();
			if(queryString.compareTo("0")==0) break;
			dbObj.queryType(queryString);
			System.out.println("");
		}
		inputScanner.close();
	}
	/**
	 * This method takes query string as argument and calls select or create method appropriately 
	 * @param query
	 */
	void queryType(String query){
		StringBuilder tempString = new StringBuilder("");
		int queryLength = query.length();
		char ch;
		/*start from i=1 since '\n' will be included in the query*/
		for(int i = 0 ; i < queryLength ; i++){
			ch = query.charAt(i);
			if(ch == ' ')
				break;
			tempString.append(ch);
		}
		//call appropriate method based on select or create 
		if(tempString.toString().equalsIgnoreCase("select")){
			inputQueryString = query;
			selectCommand(query);
		}
		else if(tempString.toString().equalsIgnoreCase("create")){
			createCommand(query);
		}
		else{
			//System.out.println("In queryType method Query Invalid");
			System.out.println("Query Invalid");
		}
	}/**Method QueryType ended*/
	
	/**
	 * Uses SQL parser to parse the input query. 
	 * Checks if the table doesn't exists and execute the query.
	 * The execution of the query creates two files : <tablename>.data and <tablename>.csv. 
	 * An entry should be made in the system config file.
	 * Print the query tokens as specified at the end.
	 * @param query
	 */
	void createCommand(String query){
		//System.out.println("*************************************************************");
		sqlParser.sqltext = query;
		parserStatus = sqlParser.parse();
		if(parserStatus == 0){

			elementNumber = sqlParser.sqlstatements.size();
			for(int i = 0; i< elementNumber ; i++){
				if(sqlParser.parse()==0 && i==0){
					String tableName = sqlParser.sqlstatements.get(i).tables.toString();
					File tableFile = new File(""+dataPath + tableName + ".data");
					if(tableFile.exists()){
						//System.out.println("Table already exists Query Invalid");
						System.out.println("Query Invalid");	
						break;
					}
				}
				analyzeStatement(sqlParser.sqlstatements.get(i));
			}
		}
		else{
			//System.out.println("In createCommand Query Invalid");
			System.out.println("Query Invalid");
			//System.out.println(sqlParser.getErrormessage());
		}
	}

	/**
	 * Use SQL parser to parse the input query.
	 * Perform all validations (table name, attributes, datatypes, operations). 
	 * Print the query tokens as specified .
	 * @param query
	 */
	void selectCommand(String query){
		//System.out.println("*************************************************************");
		sqlParser.sqltext = query;	
		parserStatus = sqlParser.parse();
		if(parserStatus == 0 ){
			//System.out.println("In select method parsing success");
			elementNumber = sqlParser.sqlstatements.size();
			for(int i = 0 ;i < elementNumber ; i++){
				analyzeStatement(sqlParser.sqlstatements.get(i));
				//System.out.println(sqlParser.sqlstatements.get(i));
			}
		}
		else{
			//System.out.println(sqlParser.getErrormessage());
			System.out.println("Query Invalid");;
		}
	}/*SelectCommand method end*/

	/**
	 * analyzeStatement method will call to corresponding method for further processing of given query
	 * @param stmt
	 */
	 void analyzeStatement(TCustomSqlStatement stmt){
		switch(stmt.sqlstatementtype){
			case sstselect:
				analyzeSelectStatement((TSelectSqlStatement)stmt);/*cast to select statement & pass to analyzeSelectstatement method */
				break;
			case sstupdate:
				break;
			case sstcreatetable:
				analyzeCreate((TCreateTableSqlStatement) stmt); /*cast into create statement and pass to analyzaCreateStatement*/
				break;
			case sstaltertable:
				break;
			case sstcreateview:
				break;
			default:
			System.out.println(stmt.sqlstatementtype.toString());
		}
	}
	/**
	 * This method verifies the existence of table name
	 * Verify attribute types are INTEGER,FLOAT or VARCHAR()
	 * updates the config file for entry of the table
	 * @param stmt
	 */
	void analyzeCreate(TCreateTableSqlStatement stmt){
		String tableName = stmt.tables.toString();
		StringBuilder attribute = new StringBuilder();
		StringBuilder attributeType = new StringBuilder();
		Vector<String> attributeVector = new Vector<String>();
		int numberOfColumns = stmt.getColumnList().size();
		int indexOfSpace;
		String temp;
		//columnList = stmt.getColumnList();
		for(int i=0;i<numberOfColumns;i++){
			attribute.setLength(0);
			attribute.append(stmt.getColumnList().getColumn(i).toString());
			//System.out.println("Attributes:"+attribute);
			
			if(attribute.charAt(0)=='(' && attribute.charAt( attribute.length() )==')')
				attributeVector.add(attribute.toString().substring(1, attribute.length()-1));
			else
				attributeVector.add(attribute.toString());
			
			temp =attributeVector.get(i).trim();
			
			
			indexOfSpace = temp.indexOf(' ');
			attributeType.setLength(0);
			attributeType.append(temp.substring(indexOfSpace+1, temp.length()));
			//System.out.println(attributeType);
			if(!isValidAttribute(attributeType))
			{
				//System.out.println("Query Invalid in  analyzeCreate");
				System.out.println("Query Invalid");
				return;
			}
		}
		
		try{
			File tableDataFile = new File(dataPath + tableName + ".data");
			File tableCsvFile = new File(dataPath + tableName + ".csv");
			if(!tableDataFile.exists() && !tableCsvFile.exists()){
		
				/*Create query success print the table name and attributes*/
				System.out.println("QueryType:create");
				System.out.println("TableName:"+tableName);
				System.out.print("Attributes:");//+stmt.getColumnList());
				int vectLength = attributeVector.size();
				attribute.setLength(0);
				LinkedHashMap<String,String> tempColumnMap = new LinkedHashMap<String,String>();
				String columnName = null,colAttributeType=null;
				for(int vectIndex=0;vectIndex<vectLength;vectIndex++){
					//attribute.setLength(0);
					indexOfSpace = attributeVector.get(vectIndex).indexOf(' ');
					columnName = attributeVector.get(vectIndex).substring(0, indexOfSpace);
					colAttributeType = attributeVector.get(vectIndex).substring(indexOfSpace+1,attributeVector.get(vectIndex).length());
					attribute.append(columnName);
					attribute.append(":");
					attribute.append(colAttributeType);
					
					if(vectIndex<vectLength-1)
						attribute.append(",");
					/*Make new entry in table HashMap along with column Name*/
					tempColumnMap.put(columnName,colAttributeType.toUpperCase());
				}
				tableWithColumnNames.put(tableName,tempColumnMap);
				/*write the data into file*/			
				tableDataFile.createNewFile();
				tableCsvFile.createNewFile();
				BufferedWriter tableDataWriter = new BufferedWriter(new FileWriter(tableDataFile));
				tableDataWriter.write(attribute+"\n");
				tableDataWriter.close();
				
				BufferedWriter configWriter = new BufferedWriter(new FileWriter(filePath,true));
				configWriter.write("BEGIN\n"+tableName+"\n");
				//StringBuilder tempString = new StringBuilder();
		
				configWriter.write(attribute.toString().replace(",","\n").replace(":", ",")+"\n");
				configWriter.write("END\n");
				configWriter.close();
				
				System.out.println(attribute.toString().replace(":", " "));
				attributeVector.clear();
				
			}
			else{
			//	System.out.println("AnalyzeCreate Table already exists Query Invalid");
				System.out.println("Query Invalid");
			}
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
	/**
	 * This method checks for validity of attribute type 
	 * if attribute is valid return true otherwise return false
	 * @param attributeType
	 * @return
	 */
	boolean isValidAttribute(StringBuilder attributeType){
		//String attType = attributeType.toString().toLowerCase();
		String attType = attributeType.toString().toUpperCase();
		//if(attType.charAt(0)=='i' || attType.charAt(0)=='f')
		if(attType.charAt(0)=='I' || attType.charAt(0)=='F')
		{
			//if(attType.equals("integer") || attType.equals("float"))
			if(attType.equalsIgnoreCase("INTEGER") || attType.equalsIgnoreCase("FLOAT"))
				return true;
			else 
				return false;
		}
		else{ //check for varchar
			//int lastIndex = attType.indexOf("varchar(");
			int lastIndex = attType.indexOf("VARCHAR");
			/*if(lastIndex>=0){
				int index = attType.indexOf('(');
				for(int i = index+1;i < attType.length()-1;i++){
					if(attType.charAt(i)>='0' && attType.charAt(i)<='9');//do nothing continue
					else return false;
				}*/
			if(lastIndex>=0)
				return true;

			else return false;
		}
	}
	/**
	 * This method analyzes the select statement with various clauses
	 * @param stmt
	 */
	void analyzeSelectStatement(TSelectSqlStatement stmt){
		//System.out.println(stmt);	
		if(stmt.tables != null )
		{
			/*validate the tables*/
			if(!isValidTables(stmt.tables)){
				//System.out.println("In analyzeSelectStatment Table does not exist Query Invalid");
				System.out.println("Query Invalid");
				return;
			}
			/*validate the columns*/
			if(!isValidColumn(stmt.getResultColumnList(),stmt.tables ) ){
				
				//System.out.println("In analyzeSelectStatment column mismatch Query Invalid");
				System.out.println("Query Invalid");
				return;				
			}
			else{
				if(stmt.getResultColumnList().size()==1 && stmt.getResultColumnList().getResultColumn(0).toString().equals("*") ){
					
					printAllColumns(stmt.tables,false);
					//System.out.println("print all columns");
				}
				else{
					printColumns(stmt.tables,stmt.getResultColumnList(),false);//+stmt.getResultColumnList());
				}
			}
			/*TODO Distinct query*/
			if(!isValidateDistinct(inputQueryString,stmt.tables)){
				//System.out.println("Failed in Distinct");
				System.out.println("Query Invalid");
				return;
				
			}
			/*validate where condition*/
			if(stmt.getWhereClause()!=null && !validateWhereCondition(stmt.tables,stmt.getWhereClause().getCondition(),false)){
				//System.out.println("Failed in where Condition");
				System.out.println("Query Invalid");
				return;
			}
			if(stmt.getOrderbyClause()!=null && !validateOrderBy(stmt.getOrderbyClause().getItems(),stmt.tables)){
				//System.out.println("Failed in OrderBy");
				System.out.println("Query Invalid");
				return;
			}
			
			if(stmt.getGroupByClause()!=null){
				if(!isValidateGroupBy(stmt.getGroupByClause().getItems())){
					//System.out.println("Group by clause failed");
					System.out.println("Query Invalid");
					return ;
				}
				if(stmt.getGroupByClause().getHavingClause()!=null){
					if(!isValidateHaving(stmt.getGroupByClause().getHavingClause(),stmt.tables)){
						//System.out.println("Having failed");
						System.out.println("Query Invalid");
						return;
					}
				}
			}
			boolean conditionFlag=true;
			/*Print all data*/
		//	System.out.println("QueryType:select");
		//	System.out.println("TableName:"+ stmt.tables);
		//	System.out.print("Columns:");
			if(stmt.getResultColumnList().size()==1 && stmt.getResultColumnList().getResultColumn(0).toString().equals("*") ){
				columnNumSet.clear();
				printAllColumns(stmt.tables,true);
				//System.out.println("print all columns");
			}
			else{
				columnNumSet.clear();
				printColumns(stmt.tables,stmt.getResultColumnList(),true);//+stmt.getResultColumnList());
			}
		//	System.out.print("Distict:");
			if(stmt.getSelectDistinct()!=null){
				//System.out.println(stmt.getSelectDistinct().getEndToken());
				//System.out.println(stmt.getSelectDistinct().getEndToken());
				//Iterator<String> it = distinctAttributeSet.iterator();
				//while()
		//		System.out.println(distinctAttributeSet);
			}
			else ;///System.out.println("NA");
			
			//System.out.print("Condition:");
			whereConditionMap.clear();
			if(stmt.getWhereClause()!=null){
				//System.out.println(stmt.getWhereClause().getCondition());
				whereConditionMap = getConditionData(stmt.getWhereClause().getCondition());
				if(stmt.getWhereClause().getCondition().toString().toLowerCase().contains(" and "))
					conditionFlag = false;
				else
					conditionFlag = true;
			}
			else{
				//System.out.println("NA");
			}
			
			//System.out.print("Orderby:");
			orderByColNumMap.clear();
			if(stmt.getOrderbyClause()!=null){
				//System.out.println(stmt.getOrderbyClause().getItems());
				/*Add the order by clause columns*/
				TOrderByItemList tempOrderList = stmt.getOrderbyClause().getItems();
				int size = tempOrderList.size();
				int colNum;
				for(int i=0;i<size;i++){
					colNum = columnNumberMap.get(tempOrderList.getElement(i).toString());
					orderByColNumMap.put(colNum,columnDataTypeMap.get(colNum));
				}
			}
			else;// System.out.println("NA");
			
			//System.out.print("Groupby:");
			if(stmt.getGroupByClause()!=null){
			//	System.out.println(stmt.getGroupByClause().getItems());
			}
			else;// System.out.println("NA");
			//System.out.print("Having:");
			if(stmt.getGroupByClause()!=null && stmt.getGroupByClause().getHavingClause()!=null){
				
				//System.out.println(stmt.getGroupByClause().getHavingClause());
			}
			else;// System.out.println("NA");
	
			int c = tableWithColumnNames.get(stmt.tables.toString()).size();
			//columnNumSet.clear();
			db3Obj.executeSelect(conditionFlag,stmt.tables,columnNumSet,c,whereConditionMap,orderByColNumMap,dataPath);
		}
		else {
			//System.out.println("In alalyzeSelectStatement Query Invalid");
			System.out.println("Query Invalid");
		}
	}	
	
	boolean isValidTables(TTableList stmtTables){
		StringBuilder tables = new StringBuilder();
		int numberOfTables = 1 ;
		tables.append(stmtTables.toString());
		numberOfTables = stmtTables.size();
		String []tableNameArray = new String[numberOfTables];
		tableNameArray = tables.toString().split(",");
		String []tempTableName = {"","",""}; /*table_name AS Alias_name*/
		if(tableWithColumnNames.size()<=maxTableCount){
			for(int i=0;i<numberOfTables;i++){
				tempTableName = tableNameArray[i].split(" ");
				if( !tableWithColumnNames.containsKey(tempTableName[0]))
					return false;
			}
		}
		else{
			//tableWithColumnNames.clear();
			/*TODO fetch the table data from configFile & search for valid table
			 * keep number of tables in main memory equal to maxCount & search then get next tableNames & so on*/
		}
		return true;
	}
	boolean isValidColumn(TResultColumnList columnList,TTableList stmtTables){

		int length = stmtTables.size();
		HashSet<String> tableHashSet = new HashSet<String>();
		for(int i=0;i<length;i++){
			tableHashSet.add(stmtTables.getTable(i).toString());
		}
		
		int columnLenght = columnList.size();
		String []tableNameArray = new String[length];
		tableNameArray = stmtTables.toString().split(",");
		StringBuilder columnString = new StringBuilder();
		int columnSplitLength;
		String []tempColumn = {"",""};
		
		/*HashSet for printing the columns*/
		printColumnSet.clear();
		String str;
		for(int index=0;index<length;index++){
			for(int i=0;i<columnLenght;i++){
				if(columnLenght==1 && columnList.getResultColumn(i).toString().equals("*"))
					return true;
				else{
					columnString.setLength(0);
					columnString.append(columnList.getResultColumn(i).toString());
					str = removeParenthesis(columnString);
					columnString.setLength(0);
					columnString.append(str);
					tempColumn = columnString.toString().split("\\.");
					columnSplitLength = tempColumn.length;//columnString.toString().split("\\.").length;
					if(columnSplitLength==1 && tableWithColumnNames.get(tableNameArray[index]).containsKey( columnString.toString() ) ){//do nothing continue with next col
						printColumnSet.add(columnString.toString());
					}
					else if(columnSplitLength>1 && tableHashSet.contains(tempColumn[0]) &&  tableWithColumnNames.get( tempColumn[0] ).containsKey( tempColumn[1] )){//do nothing continue with next col
						printColumnSet.add(columnString.toString());
					}
					//else return false;
				}
			}
		}
		for(int i=0;i<columnLenght;i++){
			columnString.setLength(0);
			columnString.append(columnList.getResultColumn(i).toString());
			str = removeParenthesis(columnString);
			columnString.setLength(0);
			columnString.append(str);
			if(!printColumnSet.contains(columnString.toString()))
				return false;
			
		}
		return true;
	}
	boolean validateWhereCondition(TTableList stmtTables,TExpression condition,boolean havingFlag){
		//System.out.println("Validate where condition  " + condition);
		/*col1 = col2 AND col3=col4*/
		TResultColumnList conditionColumnList = new TResultColumnList();
		String []columnAttribute = {"","",""};
		boolean intFlag=true,floatFlag=true;
		//StringBuilder numberString = new StringBuilder();
		for(String str:condition.toString().split("AND|OR|and|or")){
			intFlag=false;floatFlag=false;
			//System.out.println("*******"+str);
			if((str.indexOf("<>")>=0)){
				columnAttribute = str.split("<");
				columnAttribute[1] = columnAttribute[1].substring(1);
			}
			else if((str.indexOf("!=")>=0))
			{
				columnAttribute = str.split("!");
				columnAttribute[1] = columnAttribute[1].substring(1);
			}
			else
			columnAttribute = str.split("=|<=|>=|<|>|<>");
			if(columnAttribute.length>1){
				//	System.out.println(columnAttribute[0]+ " "+ columnAttribute[1]);
					if(columnAttribute[0].charAt(0)=='(')
					columnAttribute[0] = columnAttribute[0].substring(1);
					if(columnAttribute[1].charAt(columnAttribute[1].length()-1)==')')
					columnAttribute[1] = columnAttribute[1].substring(0, columnAttribute[1].length()-1);
					conditionColumnList.addResultColumn(columnAttribute[0]);
					/*TODO 
					 * column names of type table1.col1 need to be handled
					 * */
					
					try{
//						numberString.setLength(0);
						columnAttribute[0] = columnAttribute[0].trim();//removeSpace(columnAttribute[0]).toString();
						columnAttribute[1] = columnAttribute[1].trim();//removeSpace(columnAttribute[1]).toString();
						//numberString.append(removeSpace(columnAttribute[1]));
						//System.out.println(columnAttribute[1]+"BBBBBBBBB");
						Integer.parseInt(columnAttribute[1]);
						intFlag = true;
					}
					catch(NumberFormatException e){
						/*ignore the exception the string is not an integer*/
					}
					try{
						Float.valueOf(columnAttribute[1]);
						floatFlag = true;
					}
					catch(NumberFormatException e){
						
					}
					if(!intFlag && !floatFlag){
						conditionColumnList.addResultColumn(columnAttribute[1]);
						columnAttribute[0] = checkColumn(columnAttribute[0]);
						columnAttribute[1] = checkColumn(columnAttribute[1]);
						if(havingFlag){ 
							if(!isString(columnAttribute[1]) && (!havingMap.containsKey(columnAttribute[0]) || !havingMap.containsKey(columnAttribute[1]) ) )
							 return false;
							else if(isString(columnAttribute[1]) && getAttributeType(columnAttribute[0], stmtTables).indexOf("VARCHAR")!=0)
								return false;
						}
						else if(isString(columnAttribute[1])){
							if(getAttributeType(columnAttribute[0], stmtTables).indexOf("VARCHAR")!=0)/*VARCHAR not present*/
								return false;
						}
						else if(!checkColumnCompatible(columnAttribute[0],columnAttribute[1],stmtTables))
							return false;
						//System.out.println("column present");
					}
					else{/*either integer value or float value is present*/
						columnAttribute[0] = checkColumn(columnAttribute[0]);
						if(havingFlag && ( !havingMap.containsKey(columnAttribute[0]) ) )
							 return false;
						if(intFlag && !floatFlag){
							if (isColumnComparable(columnAttribute[0],stmtTables,"INTEGER") ){
							//	System.out.println("Integer only");
							}
							else /*attribute is not compatible*/ 
								return false;
						}
						else if(intFlag && floatFlag){
							if (isColumnComparable(columnAttribute[0],stmtTables,"INTEGER") || isColumnComparable(columnAttribute[0],stmtTables,"FLOAT") ){
								//System.out.println("Integer & float");
							}
							else /*attribute is not compatible*/
								return false;
						}
						else{
							if(!isColumnComparable(columnAttribute[0],stmtTables,"FLOAT"))
								return false;
							//System.out.println("float only");
						}
					}
					
			}
			else{
				/*TODO String variable is present
				 * check for LIKE,add column to conditionColumnList
				 * */
				columnAttribute = str.split("LIKE|like");
				if(columnAttribute.length>1){
					
					columnAttribute[0] = columnAttribute[0].trim();//removeSpace(columnAttribute[0]).toString();
					columnAttribute[1] = columnAttribute[1].trim();////removeSpace(columnAttribute[1]).toString();
					columnAttribute[0] = checkColumn(columnAttribute[0]);
					if(havingFlag && havingMap.containsKey(columnAttribute[0]) )
						return false;
					if(isString(columnAttribute[1]) && getAttributeType(columnAttribute[0], stmtTables).indexOf("VARCHAR")!=0)/*VARCHAR not present*/
					return false;
				}
				else return false;
			}
		}
		if(!isValidColumn(conditionColumnList, stmtTables))/*if given columns in where condition does not belong to one of the tables*/
			return false;
		return true;
	}
	boolean validateOrderBy(TOrderByItemList orderByList,TTableList stmtTables){
		TResultColumnList tempList = new TResultColumnList();
		int size = orderByList.size();
		String tempColString;
		for(int i=0;i<size;i++){
			tempColString = orderByList.getOrderByItem(i).toString();
			//System.out.println(tempColString);
			tempList.addResultColumn(tempColString);
		}
		if(!isValidColumn(tempList, stmtTables))
		return false;
		return true;
	}
	void printAllColumns(TTableList stmtTables,boolean printFlag)
	{
		int length = stmtTables.size();
		String []tableNameArray = new String[length];
		tableNameArray = stmtTables.toString().split(",");
		String tempStr = null;
		LinkedHashSet<String> columnNames = new LinkedHashSet<String>();
		groupBySet.clear();
		int cnt=0;
		for(int j=0;j<length;j++){
			tempStr = tableNameArray[j];
			Iterator<String> it1 = tableWithColumnNames.get(tempStr).keySet().iterator();
		//	LinkedHashMap<String,String> h1= tableWithColumnNames.get(tempStr); 
			while(it1.hasNext()){
				columnNames.add(it1.next());
			}			
		}
		Iterator<String> columnIt = columnNames.iterator();
		String temp;
		if(printFlag)
			columnNumberMap.clear();
		while(columnIt.hasNext())
		{
			temp = columnIt.next();
			if(printFlag){
				System.out.print("\""+temp+"\"");
				columnNumSet.add(cnt);
				columnNumberMap.put(temp,cnt);
				columnDataTypeMap.put(cnt, tableWithColumnNames.get(stmtTables.toString()).get(temp) );
			}
			groupBySet.put(temp,cnt++);
			if(columnIt.hasNext() && printFlag)
				System.out.print(",");
		}
		if(printFlag)
		System.out.println("");
	}
	void printColumns(TTableList stmtTables,TResultColumnList columnList,boolean printFlag){
		int length = columnList.size();
		StringBuilder columnString = new StringBuilder();
		String str;
		groupBySet.clear();
		int cnt=0;
		
		if(printFlag){
			try{
					columnNumberMap.clear();
					BufferedReader tableData = new BufferedReader(new FileReader(dataPath+stmtTables+".data"));
					String []columnAtt = {"",""};
					int columnCnt = 0;
					for(String dataString : tableData.readLine().split(",")){
						columnAtt = dataString.split(":");
						columnNumberMap.put(columnAtt[0], columnCnt);
						columnDataTypeMap.put(columnCnt++,columnAtt[1]);
					}
					tableData.close();
			}
			catch(IOException e){
				e.printStackTrace();
			}
		}
		for(int i=0;i<length;i++){
			columnString.setLength(0);
			columnString.append(columnList.getResultColumn(i).toString());
			str = removeParenthesis(columnString);
			columnString.setLength(0);
			columnString.append(str);
			str = checkColumn(str); /*add both tableName.colName & colName in groupBySet*/
			groupBySet.put(str,cnt++);groupBySet.put(columnString.toString(),cnt++);
			if(printFlag){
				columnNumSet.add(columnNumberMap.get(columnString.toString()));
				System.out.print("\""+columnString+"\"");
			}
			if(i<(length-1) && printFlag)
				System.out.print(",");
		}
		if(printFlag)
		System.out.println("");
	}
	String removeParenthesis(StringBuilder columnString){
		String temp=null;
		if(columnString.charAt(0)=='(' && columnString.charAt(columnString.length()-1)==')'){
			temp = columnString.toString().substring(1,columnString.length()-1);
		}
		else
			temp = columnString.toString();
		return temp;
	}
	boolean isColumnComparable(String column,TTableList stmtTables,String attType){
		if(getAttributeType(column,stmtTables).equalsIgnoreCase(attType))
		return true;
		else return false;
	}
	/**
	 * Checks the columns1 & column2 have same datatypes
	 * @param column1
	 * @param column2
	 * @param stmtTables
	 * @return
	 */
	boolean checkColumnCompatible(String column1,String column2,TTableList stmtTables){
		if(getAttributeType(column1, stmtTables).equals("NULL") || getAttributeType(column2, stmtTables).equals("NULL"))
			return false;
		
		if(getAttributeType(column1, stmtTables).equalsIgnoreCase(getAttributeType(column2, stmtTables)))
		return true;
		else return false;
	}
	String getAttributeType(String column,TTableList stmtTables){
		int tableNum = stmtTables.size();
		String tableName;
		for(int index=0;index<tableNum;index++){
			tableName= stmtTables.getTable( index).toString();
			if( tableWithColumnNames.containsKey( tableName ) ){
				if(tableWithColumnNames.get(tableName).containsKey(column))
				{
					return tableWithColumnNames.get(tableName).get(column);
				}
			}
		}
		return "NULL";
	}
	StringBuilder removeSpace(String columnAttribute){
		int l = columnAttribute.length();
		char ch;
		StringBuilder numberString = new StringBuilder();
		numberString.setLength(0);
		for(int k=0;k<l;k++){
			ch = columnAttribute.charAt(k);
			if(ch!=' ')
				numberString.append(ch);
		}
		return numberString;
	}
	/**
	 * This method return the column name from <tableName.colName>
	 * @param column
	 * @return
	 */
	String checkColumn(String column){
		int columnSplitLength ;
		String []tempColumn ={"",""};
		StringBuilder columnString = new StringBuilder();
		columnString.append(column);
		tempColumn = columnString.toString().split("\\.");
		columnSplitLength = tempColumn.length;//columnString.toString().split("\\.").length;
		if(columnSplitLength>1 && tableWithColumnNames.containsKey(tempColumn[0]) &&  tableWithColumnNames.get( tempColumn[0] ).containsKey( tempColumn[1] )){
			return tempColumn[1];//return column Name
		}
		return tempColumn[0];//return column Name
	}
	boolean isValidateDistinct(String query,TTableList stmtTables){
		//query = "select  distinct(col1,col2,col3) from table1 group by col1,col2 having col2=shrikant;";
		int distictIndex = query.toLowerCase().indexOf("distinct")+8;
		int length = query.length();
		
		char ch;
		ch=query.charAt(distictIndex);
		StringBuilder distictAttributes = new StringBuilder();
		if(ch==' ')
		{	
			for(int i=distictIndex+1;i<length;i++){
				ch=query.charAt(i);
				if(ch==',') break;
				distictAttributes.append(ch);						
			}
		}
		else if(ch=='(')
		{
			for(int i=distictIndex+1;i<length;i++){
				ch=query.charAt(i);
				if(ch==')') break;
				distictAttributes.append(ch);
				
			}
		}
		TResultColumnList tempList = new TResultColumnList();
		distinctAttributeSet.setLength(0);
		distinctAttributeSet.append(distictAttributes);
		for(String  str:distictAttributes.toString().split(",")){
			tempList.addResultColumn(str);
			//System.out.println(str);
		}
		if(isValidColumn(tempList, stmtTables))
		return true;
		else return false;
	}
	
	boolean isValidateGroupBy(TGroupByItemList groupList){
		int length = groupList.size();
		int cnt=0;
		havingMap.clear();
		for(int i=0;i<length;i++){
			//System.out.println("**"+groupList.getGroupByItem(i)+"**");
			//System.out.println("**"+groupBySet.get(groupList.getGroupByItem(i).toString())+"**");
			havingMap.put(groupList.getGroupByItem(i).toString(),cnt++);
			if(groupBySet.get(groupList.getGroupByItem(i).toString())==null)
			//System.out.println("Failed ");
				return false;
		}
		return true;
	}
	boolean isValidateHaving(TExpression havingExpression,TTableList stmtTables){
		if(validateWhereCondition(stmtTables, havingExpression, true))
		return true;
		else return false;
	}
	boolean isString(String str){
		if( (str.charAt(0)=='\'' && str.charAt(str.length()-1)=='\'') || (str.charAt(0)=='\"' && str.charAt(str.length()-1)=='\"') )
		return true;
		return false;
	}
	LinkedHashMap<Integer,Vector<WhereClauseCondition>> getConditionData(TExpression condition){
		LinkedHashMap<Integer,Vector<WhereClauseCondition>> tempCond = new LinkedHashMap<Integer,Vector<WhereClauseCondition>>();
		String []temp = {"",""};	
		for(String condArray:condition.toString().split("AND|and|OR|or")){
			if((condArray.indexOf("<>")>=0)){
				temp = condArray.split("<");
				temp[1] = temp[1].substring(1);
			}
			else if((condArray.indexOf("!=")>=0)){
				temp = condArray.split("!");
				temp[1] = temp[1].substring(1);
			}
			else
			temp = condArray.split("=|<=|>=|<|>|<>");
			WhereClauseCondition colCondition = new WhereClauseCondition();
			if(temp.length>1){
				
				if(condArray.indexOf("<=")>=0){
					colCondition.operator="le";
				}
				else if(condArray.indexOf(">=")>=0){
					colCondition.operator="ge";
				}
				else if(condArray.indexOf("<>")>=0 || condArray.indexOf("!=")>=0){
					colCondition.operator="ne";
				}
				else if(condArray.indexOf("=")>=0){
					colCondition.operator="eq";
				}
				else if(condArray.indexOf("<")>=0){
					colCondition.operator="lt";
				}
				else if(condArray.indexOf(">")>=0){
					colCondition.operator="gt";
				}
				
			}
			else{
				temp = condArray.split("LIKE|like");
				colCondition.operator="lk";
			}
			temp[0]= temp[0].trim();
			temp[1]= temp[1].trim();
			colCondition.columnDataType = columnDataTypeMap.get(columnNumberMap.get(temp[0]));
			colCondition.value = temp[1];
			if(!tempCond.containsKey(columnNumberMap.get(temp[0]))){
				Vector<WhereClauseCondition> tempVect = new Vector<WhereClauseCondition>();
				tempVect.add(colCondition);
				tempCond.put(columnNumberMap.get(temp[0]),tempVect);
			}
			else{
				tempCond.get(columnNumberMap.get(temp[0])).add(colCondition);
			}
			
		}
			
		return tempCond;				
	}

}

class WhereClauseCondition{
	String value;
	String operator;
	String columnDataType;
}
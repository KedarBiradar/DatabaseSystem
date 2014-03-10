import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import gudusoft.gsqlparser.nodes.TTableList;
import java.io.File;

public class DBDeliverable3{

	/**
	 * TODO
	 * 1. 	need to implement index on the data files for all the tables
	 * 2. 	Need to execute the select commands
	 * 		Need to have data in at least 4-5 tables prepare data & start implementation it will be easy to do the coding & verification
	 * 3. 	Order by clause is to implement 3 functions sort based on Integer,Float,Varchar (3 compare to functions need to be implemented)
	 *		while doing sorting need to check the data type of the column & need to call appropriate function.
	 *		Need to take care more than one order by clauses 
	 * */
	void executeSelect(boolean condFlag,TTableList stmtTables,LinkedHashSet<Integer> columnNumSet,int totalColumns,LinkedHashMap<Integer,Vector<WhereClauseCondition>>whereCondMap,LinkedHashMap<Integer,String> orderByColumnMap,String dataPath){
		/*It will execute for only 1 table*/	
		stmtTables.toString();
		int totalSelectRecord=0;
		boolean isOrderBy,callOrderBy=false;
		String tableName=stmtTables.toString();
		
		DBSystem dlv1=new DBSystem();
		dlv1.readConfig(DBDeliverable2.filePath);
		dlv1.populateDBInfo();
		dlv1.initAllPageTables();
		
		int noOfFiles=0;
		
		if(orderByColumnMap.size()==0)
		{
			isOrderBy=false;
		}
		else
		{
			isOrderBy=true;
		}
		
		
		try{
			String []rows = new String[totalColumns];
			String tempStr = new String();
			int recordCount=0,offset=0,maxRecordCnt=50,writeCount=0;
			long memoryLimit=0;
			boolean isValidRecord=false;
			LinkedHashMap<Integer,Integer> tempRecordMap = new LinkedHashMap<Integer,Integer>();
			int tempInt;
			BufferedReader csvFileReader = new BufferedReader(new FileReader(dataPath+stmtTables.toString()+".csv"));
			
			if(whereCondMap.size()!=0)
			{
				TreeMap<Integer,Integer> tempMap = executeWhereClause(condFlag,dataPath,stmtTables,whereCondMap,maxRecordCnt);
				writeCount = tempMap.size();
				totalSelectRecord=writeCount;
				orderBy.orderColumns=orderByColumnMap;
				
				if(writeCount>0)
				{
					String row;
					String [] columns;					
					Iterator<Integer> tempIt = tempMap.keySet().iterator();
					TreeSet<orderBy>sortedList=new TreeSet<>();
					while(tempIt.hasNext())
					{
						tempInt = tempIt.next();	
						if(isOrderBy)
						{	
							row=dlv1.getRecord(tableName, tempInt);
							memoryLimit+=row.length()+1;		
							if(memoryLimit>(DBSystem.numberOfPages*DBSystem.pageSize))
							{
								memoryLimit=0;
								callOrderBy=true;
								BufferedWriter tempFileWriter=new BufferedWriter(new FileWriter(dataPath+"tmp_file"+noOfFiles++));
								for(orderBy line : sortedList )
								{
									tempFileWriter.write(line.getRow()+"\n");
								}	
								sortedList.clear();
								tempFileWriter.flush();
								tempFileWriter.close();
								
								orderBy obj=new orderBy(row);
								sortedList.add(obj);
							}
							else
							{
								orderBy obj=new orderBy(row);
								sortedList.add(obj);
							}
						}	
						else
						{		
							row=dlv1.getRecord(tableName, tempInt);
							columns=row.split(",");	
							Iterator<Integer> it = columnNumSet.iterator();
								while(it.hasNext())
								{
									System.out.print("\""+removeQuotes(columns[it.next()])+"\"");
									if(it.hasNext())
										System.out.print(",");
								}
								System.out.println("");
						}
					}
					
					if(memoryLimit>0)
					{		
						if(isOrderBy && callOrderBy)
						{
							BufferedWriter tempFileWriter=new BufferedWriter(new FileWriter(dataPath+"tmp_file"+noOfFiles++));
							for(orderBy line : sortedList )
							{
								tempFileWriter.write(line.getRow()+"\n");
							}
							tempFileWriter.flush();
							tempFileWriter.close();
						}
						else if(isOrderBy && ! callOrderBy)
						{
							for(orderBy line : sortedList )
							{
								row=line.getRow();
								columns=row.split(",");
								Iterator<Integer> it = columnNumSet.iterator();
									while(it.hasNext())
									{
										System.out.print("\""+removeQuotes(columns[it.next()])+"\"");
										if(it.hasNext())
											System.out.print(",");
									}
									System.out.println("");
							}
						}
						sortedList.clear();
						tempMap.clear();
					}	
				}
			}
			
			//without where clause
			else
			{
				TreeSet<orderBy>sortedList=new TreeSet<>();
				orderBy.orderColumns=orderByColumnMap;
				tempStr = csvFileReader.readLine();
				
				String row;
				String [] columns;
				
				while(tempStr!=null)
				{
					rows = tempStr.toString().split(",");
					memoryLimit+=tempStr.length()+1;
					
					isValidRecord = true;
					if(isValidRecord)
					{		
						totalSelectRecord++;
						tempRecordMap.put(recordCount, offset);
						writeCount++;
						
						if(memoryLimit>(DBSystem.numberOfPages*DBSystem.pageSize))
						{
							memoryLimit=0;
												
							Iterator<Integer> tempIt = tempRecordMap.keySet().iterator();
							while(tempIt.hasNext())
							{
								tempInt = tempIt.next();
								if(isOrderBy)
								{	
									orderBy obj=new orderBy(dlv1.getRecord(tableName,tempInt));
									sortedList.add(obj);
								}	
								else
								{	
									row=dlv1.getRecord(tableName, tempInt);
									columns=row.split(",");	
									Iterator<Integer> it = columnNumSet.iterator();
										while(it.hasNext())
										{
											System.out.print("\""+removeQuotes(columns[it.next()])+"\"");
											if(it.hasNext())
												System.out.print(",");
										}
										System.out.println("");
									//System.out.println(dlv1.getRecord(tableName, tempInt));
								}
							}
							if(isOrderBy)
							{	
								callOrderBy=true;
								BufferedWriter tempFileWriter=new BufferedWriter(new FileWriter(dataPath+"tmp_file"+noOfFiles++));
								for(orderBy line : sortedList )
								{
									tempFileWriter.write(line.getRow()+"\n");
								}	
								sortedList.clear();
								tempRecordMap.clear();
								tempFileWriter.flush();
								tempFileWriter.close();
							}	
						}
					}
					recordCount++;
					offset += tempStr.length()+1;
					tempStr = csvFileReader.readLine();	
				}
				if(memoryLimit>0 )
				{
					Iterator<Integer> tempIt = tempRecordMap.keySet().iterator();
					while(tempIt.hasNext())
					{
						tempInt = tempIt.next();
						if(isOrderBy)
						{	
							//orderBy.orderColumns=orderByColumnMap;				
							orderBy obj=new orderBy(dlv1.getRecord(tableName,tempInt));
							sortedList.add(obj);
						}	
						else
						{
							row=dlv1.getRecord(tableName, tempInt);
							columns=row.split(",");
							
							Iterator<Integer> it = columnNumSet.iterator();
								while(it.hasNext())
								{
									System.out.print("\""+removeQuotes(columns[it.next()])+"\"");
									if(it.hasNext())
										System.out.print(",");
								}
								System.out.println("");
							//System.out.println(dlv1.getRecord(tableName, tempInt));
						}
					}
					
					if(isOrderBy && callOrderBy)
					{	
						BufferedWriter tempFileWriter=new BufferedWriter(new FileWriter(dataPath+"tmp_file"+noOfFiles++));
						for(orderBy line : sortedList )
						{
							tempFileWriter.write(line.getRow()+"\n");
						}
						tempFileWriter.flush();
						tempFileWriter.close();
					}
					
					else if(isOrderBy && ! callOrderBy)
					{
						for(orderBy line : sortedList )
						{
							row=line.getRow();
							columns=row.split(",");
							Iterator<Integer> it = columnNumSet.iterator();
								while(it.hasNext())
								{
									System.out.print("\""+removeQuotes(columns[it.next()])+"\"");
									if(it.hasNext())
										System.out.print(",");
								}
								System.out.println("");
						}
					}
					sortedList.clear();
					tempRecordMap.clear();
				}
			}
			csvFileReader.close();
		}
		catch(IOException e)
		{
				e.printStackTrace();
		}	
		
		if(isOrderBy && callOrderBy)
		{	
			OrderByImplement(stmtTables, columnNumSet, orderByColumnMap, dataPath, totalSelectRecord,dlv1,noOfFiles);
		}
		
		//System.out.println();
	}
	void createIndex(Set<String> tableNames,String dataPath){
		TreeMap<Integer,String> colAttribute = new TreeMap<Integer,String>();
		String tempStr;
		String []col = {"",""};
		int size,recordNum=0,size1,recLength=0 ;
		int writeCount=0,maxCount=1000;
		new File(dataPath+"tempOutputDir").mkdir();
		String dataPath1 = dataPath+"tempOutputDir/";
		for(String table : tableNames){
			try{
				BufferedReader readFile = new BufferedReader(new FileReader(dataPath+table+".data"));
				BufferedReader readFileData = new BufferedReader(new FileReader(dataPath+table+".csv"));
				tempStr = readFile.readLine();
				colAttribute.clear();
				int cnt=0;
				for(String str : tempStr.split(",")){
					col = str.split(":");
					colAttribute.put(cnt++, col[1]);
				}
				readFile.close();
				size = colAttribute.size();
				HashMap<Integer,TreeMap<AttValue,Vector<Integer>>> indexMap = new HashMap<Integer, TreeMap<AttValue,Vector<Integer>>>();
				HashMap<Integer,Integer> recordLengthMap = new HashMap<Integer,Integer>();
				int []dataType = new int[size];
				BufferedWriter []writeIndex = new BufferedWriter[size];
				for(int i=0;i<size;i++){
					if(colAttribute.get(i).compareToIgnoreCase("integer")==0){
						dataType[i] = 0;
					}
					else if(colAttribute.get(i).compareToIgnoreCase("float")==0){
						dataType[i] = 1;
					}
					else {//if(colAttribute.get(i).indexOf("varchar")==0 || colAttribute.get(i).indexOf("VARCHAR")==0){
						dataType[i] = 2;
					}					
					writeIndex[i] = new BufferedWriter(new FileWriter(dataPath1+table+""+i+"Index"));
				}
				tempStr = readFileData.readLine();
				String []columnArray = new String[size];
				AttValue tempAttribute;
				TreeMap<AttValue,Vector<Integer>> tempMap;// = new TreeMap<AttValue,Integer>();
				Vector<Integer> tempVect;
				writeCount = 0;
				recordNum = 0;
				while(tempStr !=null){
					columnArray = tempStr.split(",");
					for(int i=0;i<size;i++){
						if(dataType[i]==0){
							AttValue.dataType = 0;
						}
						else if(dataType[i]==1){
							AttValue.dataType = 1;
						}
						else{
							AttValue.dataType = 2;
						}
						tempAttribute = new AttValue();
						columnArray[i] = removeQuotes(columnArray[i]);
						tempAttribute.value = columnArray[i];
						tempMap = new TreeMap<AttValue,Vector<Integer>>();
						
						if(indexMap.get(i)==null){
							tempVect = new Vector<Integer>();
							tempVect.add(recordNum);
							tempMap.put(tempAttribute,tempVect);
							indexMap.put(i, tempMap);
						}
						else{
							if(!indexMap.get(i).containsKey(tempAttribute)){
								tempVect = new Vector<Integer>();
								tempVect.add(recordNum);
								indexMap.get(i).put(tempAttribute,tempVect);
							}
							else{
								indexMap.get(i).get(tempAttribute).add(recordNum);
							}
							
						}												
					}
					
					writeCount++;
					if(writeCount==maxCount){
						writeCount=0;
						for(int i=0;i<size;i++){
							AttValue.dataType=i;
							tempMap = indexMap.get(i);
							Iterator<AttValue> it = tempMap.keySet().iterator();
							while(it.hasNext()){
								tempAttribute = it.next();
								//tempAttribute.flag = false;
								String temp = tempAttribute.value;
								tempVect = tempMap.get(tempAttribute);
								//System.out.println(tempAttribute);
								size1 = tempVect.size();	
								writeIndex[i].write(temp+"#");
								for(int j=0;j<size1;j++){
									recLength = tempVect.get(j);
									writeIndex[i].write(recLength+"@"+recordLengthMap.get(recLength)+",");
								}
								writeIndex[i].write("\n");
							}
						}
						recordLengthMap.clear();
						indexMap.clear();
					}
					recordLengthMap.put(recordNum++,tempStr.length()+1);
					tempStr = readFileData.readLine();
				}
				if(writeCount>0){
					writeCount=0;
					for(int i=0;i<size;i++){
						AttValue.dataType=dataType[i];
						tempMap = indexMap.get(i);
						Iterator<AttValue> it = tempMap.keySet().iterator();
						while(it.hasNext()){
							tempAttribute = it.next();
							//tempAttribute.flag = false;
							String temp = tempAttribute.value;
							tempVect = tempMap.get(tempAttribute);
							//System.out.println(tempAttribute);
							size1 = tempVect.size();	
							writeIndex[i].write(temp+"#");
							for(int j=0;j<size1;j++){
								recLength = tempVect.get(j);
								writeIndex[i].write(recLength+"@"+recordLengthMap.get(recLength)+",");
							}
							writeIndex[i].write("\n");
						}
					}
					recordLengthMap.clear();
					indexMap.clear();
				}
				for(int i=0;i<size;i++)
				writeIndex[i].close();
				readFileData.close();
				
			}
			catch(IOException e){
				e.printStackTrace();
			}
		}
	}
	TreeMap<Integer,Integer> executeWhereClause(boolean condFlag,String dataPath,TTableList stmtTables,LinkedHashMap<Integer,Vector<WhereClauseCondition>> whereCond,int maxCount){
		TreeMap<Integer,Integer> tempRecordMap = new TreeMap<Integer,Integer>();
		TreeMap<Integer,Integer> finalResultMap = new TreeMap<Integer,Integer>();
		TreeMap<Integer,Integer> tempMap = new TreeMap<Integer,Integer>();
		int count=0,colIndex,recordNum;
		Vector<WhereClauseCondition> tempWhreObj;
		WhereClauseCondition tempWhereObj;
		Iterator<Integer> it = whereCond.keySet().iterator();
		Iterator<Integer> tempIt;
		Iterator<WhereClauseCondition> vectIt;
		String dataPath1 = dataPath+"tempOutputDir/";
		while(it.hasNext()){
			colIndex = it.next();
			try{
				tempWhreObj = whereCond.get(colIndex);//vector of WhereClauseCondition
				vectIt = tempWhreObj.iterator();
				while(vectIt.hasNext()){
				/**********************************************************************************************/
				BufferedReader tempReader = new BufferedReader(new FileReader(""+dataPath1+stmtTables.toString()+colIndex+"Index"));
				tempWhereObj = vectIt.next();
				if(tempWhereObj.columnDataType.equalsIgnoreCase("integer")){
					tempRecordMap = getResultInt(tempReader,tempWhereObj.operator,tempWhereObj.value);
				}
				else if(tempWhereObj.columnDataType.equalsIgnoreCase("float")){
					tempRecordMap = getResultFloat(tempReader,tempWhereObj.operator,tempWhereObj.value);
				}
				else{
					tempRecordMap = getResultString(tempReader,tempWhereObj.operator,tempWhereObj.value);
				}
				
				 tempIt = tempRecordMap.keySet().iterator();
				 while(tempIt.hasNext()){
					 recordNum = tempIt.next();
					 if(count==0 || condFlag)//first cond or OR condition
						 finalResultMap.put(recordNum,tempRecordMap.get(recordNum));
					 else if(finalResultMap.containsKey(recordNum)){
						 tempMap.put(recordNum,tempRecordMap.get(recordNum));
					 }
				 }
				 if(!condFlag && count>0){
					 finalResultMap.clear();
					 tempIt = tempMap.keySet().iterator();
					 while(tempIt.hasNext()){
						 recordNum = tempIt.next();
						 finalResultMap.put(recordNum,tempMap.get(recordNum));
					 }
					 tempMap.clear();
				 }
				 count++;
				 tempReader.close();
				 /******************************************************************************/
				}
				
			}
			catch(IOException e){
				e.printStackTrace();
			}
		}
		return finalResultMap;
	}
	TreeMap<Integer,Integer> getResultInt(BufferedReader tempResult,String op,String value) throws IOException{
		TreeMap<Integer,Integer> tempMap = new TreeMap<Integer,Integer>();
		int valueF=0,tempF=0;
		valueF = Integer.parseInt(value);
		String tempStr,tempResultStr=null;
		tempStr = tempResult.readLine();
		String []tempStrArr={"",""};
		String []recordNum={"",""};
		while(tempStr!=null){
			tempStrArr = tempStr.split("#");
			tempF = Integer.parseInt(tempStrArr[0]);
			switch(op){
				case "eq":
						if(valueF==tempF)
							tempResultStr = tempStrArr[1];
						break;
				case "le":
					if(tempF<=valueF)
						tempResultStr = tempStrArr[1];
					break;
				case "ge":
					if(tempF>=valueF)
						tempResultStr = tempStrArr[1];
					break;
				case "lt":
					if(tempF<valueF)
						tempResultStr = tempStrArr[1];
					break;
				case "gt":
					if(tempF>valueF)
						tempResultStr = tempStrArr[1];
					break;
				case "ne":
					if(valueF!=tempF)
						tempResultStr = tempStrArr[1];
					break;
			}
			if(tempResultStr!=null){
				for(String str:tempResultStr.split(",")){
					recordNum = str.split("@");
					tempMap.put(Integer.parseInt(recordNum[0]), Integer.parseInt(recordNum[1]));
				}
			}
			tempResultStr=null;
			tempStr = tempResult.readLine();
		}
		return tempMap;
	}
	TreeMap<Integer,Integer> getResultFloat(BufferedReader tempResult,String op,String value) throws IOException{
		TreeMap<Integer,Integer> tempMap = new TreeMap<Integer,Integer>();
		float valueF=0,tempF=0;
		valueF = Float.parseFloat(value);
		String tempStr,tempResultStr=null;
		tempStr = tempResult.readLine();
		String []tempStrArr={"",""};
		String []recordNum={"",""};
		while(tempStr!=null){
			tempStrArr = tempStr.split("#");
			tempF = Float.parseFloat(tempStrArr[0]);
			switch(op){
				case "eq":
						if(valueF==tempF)
							tempResultStr = tempStrArr[1];
						break;
				case "le":
					if(tempF<=valueF)
						tempResultStr = tempStrArr[1];
					break;
				case "ge":
					if(tempF>=valueF)
						tempResultStr = tempStrArr[1];
					break;
				case "lt":
					if(tempF<valueF)
						tempResultStr = tempStrArr[1];
					break;
				case "gt":
					if(tempF>valueF)
						tempResultStr = tempStrArr[1];
					break;
				case "ne":
					if(valueF!=tempF)
						tempResultStr = tempStrArr[1];
					break;
			}
			if(tempResultStr!=null){
				for(String str:tempResultStr.split(",")){
					recordNum = str.split("@");
					tempMap.put(Integer.parseInt(recordNum[0]), Integer.parseInt(recordNum[1]));
				}
			}
			tempResultStr=null;
			tempStr = tempResult.readLine();
		}
		return tempMap;
	}
	TreeMap<Integer,Integer> getResultString(BufferedReader tempResult,String op,String value) throws IOException{
		TreeMap<Integer,Integer> tempRecordMap = new TreeMap<Integer,Integer>();
		String tempStr,tempResultStr=null;
		tempStr = tempResult.readLine();
		String []tempStrArr={"",""};
		String []recordNum={"",""};
		if( value.charAt(0)=='\'' || value.charAt(0)=='\"'){
			value = value.substring(1);
		}
		if(value.charAt(value.length()-1)=='\'' || value.charAt(value.length()-1)=='\"') {
			value = value.substring(0,value.length()-1);
		}
		while(tempStr!=null){
			tempStrArr = tempStr.split("#");
			tempStrArr[0] = tempStrArr[0].trim();
			switch(op){
				case "eq":
					if(value.equals(tempStrArr[0]))
						tempResultStr = tempStrArr[1];
					break;
				case "lk":
					if(value.equalsIgnoreCase(tempStrArr[0]))
						tempResultStr = tempStrArr[1];
					break;
			}
			if(tempResultStr!=null){
				for(String str:tempResultStr.split(",")){
					recordNum = str.split("@");
					tempRecordMap.put(Integer.parseInt(recordNum[0]), Integer.parseInt(recordNum[1]));
				}
			}
			tempResultStr=null;
			tempStr = tempResult.readLine();
		}
		return tempRecordMap;
	}

	public void OrderByImplement(TTableList stmtTables,LinkedHashSet<Integer> columnNumSet,LinkedHashMap<Integer,String> orderByColumnMap,String dataPath,int noOfRows,DBSystem dlv1,int noOfFiles)
	{
			MergeSortedFiles msf=new MergeSortedFiles();
			PQueueNode.orderColumns1=orderByColumnMap;
			msf.MergeFiles(noOfFiles,dataPath,columnNumSet);
			
	}

static String removeQuotes(String input)
{
		if(input.charAt(0)=='\"' && input.charAt(input.length()-1)=='\"')
		{
			input = input.substring(1);
			input = input.substring(0,input.length()-1);
		}
		return input;
}
}

class AttValue implements Comparable<AttValue>{
	String value;
	static int dataType;
	boolean flag;
	public AttValue() {
		// TODO Auto-generated constructor stub
		flag=true;
	}
	@Override
	public int compareTo(AttValue s1){
		if(s1.flag==true){
			if(dataType==0){
				return (Integer.parseInt(this.value)-Integer.parseInt(s1.value));
			}
			else if(dataType==1){
				if(Float.parseFloat(this.value)>Float.parseFloat(s1.value)){
					return 1;
				}
				else if(Float.parseFloat(this.value)==Float.parseFloat(s1.value))
					return 0;
				else return -1;
			}
			else{
				return this.value.compareTo(s1.value);
			}
		}
		else{
			return this.value.compareTo(s1.value);
		}
	}
	@Override
	public boolean equals(Object obj){
		if(obj==null) return false;
		if(obj==this) return true;
		if(getClass() != obj.getClass()) return false;
		AttValue tempObj = (AttValue) obj;
		return this.value.equals(tempObj.value);
	}
	@Override
	public int hashCode(){
		return 0;
	}
}


class MergeSortedFiles
{
    PriorityQueue<PQueueNode> queue = new PriorityQueue<PQueueNode>();
    TreeMap<Integer, Boolean> fileAvailable = new TreeMap<Integer, Boolean>();
    BufferedReader [] readers;
    String [] lines,terms;
    boolean [] readLine;
    static int fileNumber;

    public void MergeFiles(int NoOfFiles,String dataPath,LinkedHashSet<Integer> columnNumSet) 
    {

        fileNumber=NoOfFiles;
        String fileName=dataPath+"/tmp_file";
      //  BufferedWriter writerPrimaryIndexFile = null;
        StringBuilder line=null;
        try
        {
           // File file = new File(dataPath+"/FinalOutput");
          //  writerPrimaryIndexFile = new BufferedWriter(new FileWriter(file));
            readers = new BufferedReader[fileNumber+1];
            lines = new String[fileNumber+1];
            terms = new String[fileNumber+1];
            readLine = new boolean[fileNumber+1];

            for(int i =0; i < fileNumber; i++)
            {
                readers[i] = new BufferedReader(new FileReader(fileName+i));
                lines[i] = readers[i].readLine();
                PQueueNode node = new PQueueNode(lines[i],i);
                if(node!=null)
                queue.add(node);
            }

            fileAvailable = new TreeMap<Integer, Boolean>();
            for(int k = 0; k < fileNumber; k++)
            {
                fileAvailable.put(k, true);
            }
            String [] columns;
           // StringBuilder buffer=new StringBuilder();
            int ind;
            while(true)
            {

                 line = RemoveLines();

                if(line == null)
                    break;
                
                columns=line.toString().split(",");
                Iterator<Integer> it = columnNumSet.iterator();
				while(it.hasNext())
				{
					ind=it.next();
					
					System.out.print("\""+DBDeliverable3.removeQuotes(columns[ind])+"\"");
					if(it.hasNext())
						System.out.print(",");
					//buffer.append(columns[ind]+"\t");
				}
				System.out.println("");                
               // writerPrimaryIndexFile.write(buffer.toString());
               // writerPrimaryIndexFile.write('\n');
               //buffer.setLength(0);
                AddLines();
            }
            while(!queue.isEmpty())
            {
                PQueueNode head = queue.remove();
                int index = head.getIndex();
                
                columns=lines[index].toString().split(",");
                Iterator<Integer> it = columnNumSet.iterator();
				while(it.hasNext())
				{
					ind=it.next();
					System.out.print("\""+DBDeliverable3.removeQuotes(columns[ind])+"\"");
					if(it.hasNext())
						System.out.print(",");
					//buffer.append(columns[ind]+"\t");
				}
				System.out.println("");
               // writerPrimaryIndexFile.write(buffer.toString());
               // writerPrimaryIndexFile.write('\n');
               //buffer.setLength(0);
            }
            //writerPrimaryIndexFile.flush();
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
        finally
        {
        	try
            {
              //  writerPrimaryIndexFile.close();
                for(int i =0; i < fileNumber; i++)
                {
                	readers[i].close();
                	File fileToDelete = new File(fileName+i);
                	fileToDelete.delete();
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    public StringBuilder RemoveLines()
    {
        if(queue.isEmpty())
            return null;

        PQueueNode head = queue.remove();
        int index = head.getIndex();

        readLine[index] = true;
            return new StringBuilder(new String(lines[index]));
    }

    public void AddLines() throws IOException
    {
        for(int index = 0; index < fileNumber; index++)
        {
            if(readLine[index] == false || fileAvailable.get(index) == false)
                continue;

            String line = readers[index].readLine();

            if(line == null)
            {
                fileAvailable.put(index, false);
            }
            else
            {
                lines[index] = line;
                readLine[index] = false;
                PQueueNode node = new PQueueNode(lines[index].toString(),index);
                queue.add(node);
            }
        }
    }
}

class orderBy implements Comparable<orderBy>
{
	String row;
	public static LinkedHashMap<Integer, String> orderColumns;
	public orderBy(String row)
	{
		this.row = row;
	}

	public String getRow()
	{
		return row;
	}

	public void setRow(String row)
	{
		this.row = row;
	}

	@Override
	public int compareTo(orderBy arg0) 
	{
		String row2=arg0.getRow();
		String [] columns1=this.row.split(",");
		String [] columns2=row2.split(",");
		int i,columnNo;
		String columnDataType;
		Object [] key=orderColumns.keySet().toArray();
		Object [] values=orderColumns.values().toArray();
		int val1,val2,val5;
		float val3,val4;
		//DBDeliverable3 dlv3= new DBDeliverable3();
		for(i=0;i<key.length;i++)
		{
			columnNo=Integer.parseInt(DBDeliverable3.removeQuotes(key[i].toString()));
			//columnNo=dlv3.removeQuotes(columnNo);
			columnDataType=values[i].toString().toLowerCase();
			
			if(columnDataType.equals("integer"))
			{
							val1=Integer.parseInt(DBDeliverable3.removeQuotes(columns1[columnNo]));
							val2=Integer.parseInt(DBDeliverable3.removeQuotes(columns2[columnNo]));
							if(val1<val2)
								return -1;
							else if(val1>val2)
								return 1;
							else
								continue;
			}				
			else if(columnDataType.equals("float"))
			{	
							val3=Float.parseFloat(DBDeliverable3.removeQuotes(columns1[columnNo]));
							val4=Float.parseFloat(DBDeliverable3.removeQuotes(columns2[columnNo]));
							if(val3<val4)
								return -1;
							else if(val3>val4)
								return 1;
							else
								continue;
			}				
			else
			{	
							val5=DBDeliverable3.removeQuotes(columns1[columnNo]).compareTo(DBDeliverable3.removeQuotes(columns2[columnNo]));
							if(val5==0)
								continue;
							else
								return val5;				
			}
		}
		return 1;
	}	
}

class PQueueNode implements Comparable<PQueueNode> 
{
    String word;
    int index;
    public static LinkedHashMap<Integer, String> orderColumns1;

    public PQueueNode(String word, int index)
    {
        this.word = word;
        this.index = index;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public int compareTo(PQueueNode arg) 
    {
    	String row2=arg.getWord();
		String [] columns1=this.word.split(",");
		String [] columns2=row2.split(",");
		int i,columnNo;
		String columnDataType;
		Object [] key=orderColumns1.keySet().toArray();
		Object [] values=orderColumns1.values().toArray();
		int val1,val2,val5;
		float val3,val4;
		//DBDeliverable3 dlv3= new DBDeliverable3();
		
		for(i=0;i<key.length;i++)
		{
			columnNo=Integer.parseInt(DBDeliverable3.removeQuotes(key[i].toString()));
			columnDataType=values[i].toString().toLowerCase();
			if(columnDataType.equals("integer"))
			{
							val1=Integer.parseInt(DBDeliverable3.removeQuotes(columns1[columnNo]));
							val2=Integer.parseInt(DBDeliverable3.removeQuotes(columns2[columnNo]));
							if(val1<val2)
								return -1;
							else if(val1>val2)
								return 1;
							else
								continue;
			}				
			else if(columnDataType.equals("float"))
			{	
							val3=Float.parseFloat(DBDeliverable3.removeQuotes(columns1[columnNo]));
							val4=Float.parseFloat(DBDeliverable3.removeQuotes(columns2[columnNo]));
							if(val3<val4)
								return -1;
							else if(val3>val4)
								return 1;
							else
								continue;
			}				
			else
			{	
							val5=DBDeliverable3.removeQuotes(columns1[columnNo]).compareTo(DBDeliverable3.removeQuotes(columns2[columnNo]));
							if(val5==0)
								continue;
							else
								return val5;				
			}				
		}
		return 1;
    
    }
}

import java.io.File;
import java.io.IOException;
//import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.*;

//import sun.awt.image.OffScreenImage;

//import java.util.HashSet;


class MapPageTORecord
{
	int pgNumber;
	int startRecordId;
	int endRecordId;
}

class TablePageOffset
{
	int startRecord;
	int endRecord;
	int offSet;
	public TablePageOffset() {
		startRecord = 0;
		endRecord = 0;
		offSet = 0 ;
	}
}

public class DBSystem {
	/**
	 * 
	 * @param args
	 */
	static int pageSize;
	static int numberOfPages;
	static String dataPath;

	/*tableNames is a HashMap used for storing the name of table & table number*/
	private static HashMap<String,Integer> tableNames = new HashMap<String,Integer>();
	
	/*pageTable is table containing Table Name & corresponding pages,starting record number & ending record number*/
	private static HashMap<String,Vector<MapPageTORecord>> pageTable = new HashMap<String,Vector<MapPageTORecord>>();
	
	/*dbInfo maintains offsets of all the records which can fit into a page*/
	private static HashMap<String,Vector<TablePageOffset>> dbInfo = new HashMap<String,Vector<TablePageOffset>>();
	
	/*tableCount gives the total number of tables in the dataPath*/
	private static int  tableCount;
	
	/*freePages maintains the set of available pages*/
	private static Vector<Integer> freePages = new Vector<Integer>();
	
	/*totalTableSize gives the total size of all the tables together*/
	private static long totalTableSize;
	
	/*memoryMap is actual memory allocated to pages in memory*/
	private static HashMap<Integer,String>memoryMap = new HashMap<Integer,String>();

    /* Global page table holds all page and its status(free or occupied)*/
    public static HashMap<Integer,String> globalPageTable;

    /*for each table there is local page table holds page number entries of pages occupied by that table*/
    public static HashMap<String,HashMap<Integer,TablePageOffset>> localPageTable;

    /* actual page having page number and recordID and record*/
    public static LinkedHashMap<Integer,HashMap<Integer,String>> page;


    public  void initAllPageTables()
    {

        globalPageTable=new HashMap<Integer,String>(numberOfPages);
        page=new LinkedHashMap<Integer, HashMap<Integer, String>>(numberOfPages,0.75f,true);

        int i;
        for(i=0;i<numberOfPages;i++)
        {
            globalPageTable.put(i,null);
            page.put(i,null);
        }


        localPageTable=new HashMap<String, HashMap<Integer, TablePageOffset>>();

        Object [] tablename=tableNames.keySet().toArray();


        try
        {
            for(i=0;i<tableNames.size();i++)
            {
                localPageTable.put(tablename[i].toString(),null);

            }

        }
        catch(Exception e)
        {

            e.printStackTrace();
        }


        //  System.out.println(globalPageTable);
        //  System.out.println(localPageTable);


    }

    /**
     * This method is introduced in deliverable 2 for returning the private HashMap tableNames
     * @return
     */
    HashMap<String,Integer>  getTableNames(String configFilePath){
    	readConfig(configFilePath);
    	return tableNames;
    }

	/**
	 * This method reads the configFile and retrieves information like PAGESIZE,NUM_PAGES,PATH_FOR_DATA(tables)
	 * table information(name & attributes) 
	 * @param configFilePath
	 */
	void readConfig(String configFilePath)
	{
		try
		{
            		//configFilePath="/home/kedar/dbs/config.txt";
			//configFilePath = configFilePath + "config.txt";
			Scanner configBuffer = new Scanner(new File(configFilePath));
			configBuffer.useDelimiter("\n");
			String parameters ;
			parameters = configBuffer.next();
			parameters = parameters.substring(parameters.indexOf(' ')+1, parameters.length());
			pageSize = Integer.parseInt(parameters);
			
			parameters = configBuffer.next();
			parameters = parameters.substring(parameters.indexOf(' ')+1, parameters.length());
			numberOfPages = Integer.parseInt(parameters);
			
			/**
			 * commented the freePages.add(i) for loop for deliverable 2
			 */
			/*
			for(int i=0 ; i<numberOfPages;i++)/*maintain the hashSet of free pages*
				freePages.add(i);
			*/
			parameters = configBuffer.next();
			dataPath = parameters = parameters.substring(parameters.indexOf(' ')+1, parameters.length());

			
			tableCount=0;
			totalTableSize=0;
			boolean tableStartFlag = false,tableNameFlag = false;
			tableNames.clear();
			while(configBuffer.hasNext())
			{
				parameters = configBuffer.next();
				if(parameters.equals("BEGIN"))
				{
					tableStartFlag = true;
					tableNameFlag = true;
				}
				else if (tableStartFlag && parameters.equals("END"))
					tableStartFlag = false;
				else if(tableNameFlag)/*Add only table name to HashMap*/
				{
					tableNames.put(new String(parameters),tableCount++);
					//File tables = new File(dataPath + "/" + parameters + ".csv");
					File tables = new File( parameters + ".csv");
					if(tables.exists())
					totalTableSize += tables.length();
					tableNameFlag = false;
				}	
				else/*Meta data of the current table*/
				{
					
				}
			}
			configBuffer.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	/**Commented the iniAllPageTables method for deliverable 2 
	 * */
		//initAllPageTables();
	}
	/**
	 * This method maintains contents of table i.e. records & corresponding page number
	 */
	 void populateDBInfo()
	{
		/*TODO for each table start the page numbers from the 0th page
		 * maintain a table of TABLENO,RecordNO,PAGENo
		 * Maintain a mapping from PageNumber to (StartingRecordId,
		 * EndingRecordId)
		 * keep the buffer size fixed i.e. at a time fetch some amount data into buffer 
		* delimiter for the records in the page is #
		*/
		try
		{
			Iterator<String> it = tableNames.keySet().iterator();

			int currentFree=0;
			int currentOffset,currentRecord,recordLength=0;
			int nextRecordOffset=0,nextRecordSize=0;
			String record="",currentTableName ;
			boolean newPage=true,firstPage = true,pageAdded = false,lastRecord=false;
			TablePageOffset offSetObj = new TablePageOffset();
			int newLine = 0;
			while(it.hasNext())/* loop for all the tables in config File */
			{
				currentTableName = it.next();
				Scanner tableFile = new Scanner(new File(dataPath + currentTableName + ".csv"));
				tableFile.useDelimiter("\n");
				currentOffset = 0;currentRecord = 0;
				firstPage = true;
				newPage=true;
				nextRecordOffset=0;nextRecordSize=0;
				lastRecord = false ;
				newLine = 0;
				while(tableFile.hasNext())/* loop for all records in the particular table*/
				{	
					if(newPage)
					{	
						offSetObj = new TablePageOffset(); //contains the offset in file,start & end record
						offSetObj.startRecord = currentRecord;
						offSetObj.offSet = currentOffset ;
						currentOffset += nextRecordOffset;
						currentFree = pageSize-nextRecordSize;
						newLine = 1;
						newPage = false;
						pageAdded = false;
						record = null;
					}
					else
					{
						record = tableFile.next();
						recordLength = record.length();
						if(!tableFile.hasNext() && record!=null)
							lastRecord = true;
						currentOffset += (recordLength+1); /* +1 for new line character */
						if((currentFree-recordLength-1)<0 && (currentFree - recordLength)==0)
						{
							newLine = 0;						
						}
						currentFree -= (recordLength + newLine);	/* subtract record length from free page size */
						currentRecord++;
						if(currentFree<0)
						{
							if(firstPage)
							{	currentRecord--;firstPage = false;
							}
							offSetObj.endRecord = currentRecord -1;
							currentOffset -= (recordLength+1); /* offset of this record must be starting offset */
							
							nextRecordOffset = recordLength+newLine; /* this record will be added in next page*/
							nextRecordSize = recordLength+0;	/* offset of this page in file */
							
							addRecordtoDB(currentTableName,offSetObj);
							newPage = true;
							pageAdded = true;
							
						}
						newLine = 1;
						
					}
							
				}
                /*TODO if last record fits exactly into the page need to handle*/
				if(lastRecord && pageAdded)//last record and it is the first record in new page
				{
					offSetObj = new TablePageOffset(); //contains the offset in file,start & end record
					offSetObj.startRecord = currentRecord;
					offSetObj.offSet = currentOffset ;
					offSetObj.endRecord = currentRecord;
					addRecordtoDB(currentTableName, offSetObj);
				}
				else if(lastRecord && !pageAdded) //last record but it is not the first record in new page
				{
					offSetObj.endRecord = currentRecord;
					addRecordtoDB(currentTableName, offSetObj);
				}
				tableFile.close();
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		
		//printDBInfo();
	}
	/**
	 * This is an implementation of the LRU page replacement algorithm 
	 * return the record
	 * @param tableName
	 * @param recordId
	 * @return
	 */




	     String getRecord(String tableName, int recordId)
	{
		/*TODO Implementation of LRU algorithm*/

	//recordId++;


          String tableNameFile=dataPath.concat(tableName);
            tableNameFile=tableNameFile.concat(".csv");
          int i,pageIndex;

            String line="",replaceTableName="";

            int startOffset=0,endOffset=0,availablePage,startLine=0,endLine=0;

            /* search if that record ID is already available in page*/
            String found=searchRecordID(tableName,recordId);


            /* if record ID available in that page then page hit*/
            if(found!=null)
            {
              //  System.out.println("HIT");
                return found;
            }


            /* else page miss*/
            



            /*check if any free page available in global page table*/
            availablePage=freePageAvailable();

//            System.out.println("Free page found is="+availablePage);

            /* if free page not available then go for page replacement*/
            if(availablePage==-1)
            {

           //     System.out.println("Free page not available, going to page replacement");

                availablePage=replacePageAlgo();


                /* get table name from which the old entry is to be invalidated */
                replaceTableName=globalPageTable.get(availablePage);


                /* remove old page entry*/

                localPageTable.get(replaceTableName).remove(availablePage);
                //System.out.println("replaced table name="+replaceTableName+" replaced page="+availablePage);

            }
           // System.out.println("MISS "+availablePage);
            Vector <TablePageOffset> table=dbInfo.get(tableName);

              /* get start record ,end record and offset from dbInfo for reading the only that page from file*/

            for(i=0;i<table.size();i++)
            {
               if(table.get(i).startRecord <= recordId && table.get(i).endRecord>=recordId)
               {
                   startOffset=table.get(i).offSet;
                   startLine=table.get(i).startRecord;
                   endLine=table.get(i).endRecord;

                   if(i<table.size()-1)
                   endOffset=table.get(i+1).offSet;

                   break;
               }

            }

            pageIndex=i;

        //System.out.println("Start line="+startLine+"End line="+endLine);

        try
        {
             HashMap<Integer,String> buffer=new HashMap<Integer, String>();

            /* open table file to read page */
            RandomAccessFile randomAccessFile=new RandomAccessFile(tableNameFile,"r");

            /* goto first record of that page using offset*/
            randomAccessFile.seek((long)startOffset);


            /*read next n records*/
            for(i=0;i< (endLine-startLine+1);i++)
            {
                line=randomAccessFile.readLine();

                if(line==null)
                    break;

                if(startLine+i==recordId)
                    found=line;

                buffer.put(startLine+i,line);

            }
            page.put(availablePage,buffer);

             /* Add newly aquired page number to localpagetable of that table*/

            HashMap<Integer,TablePageOffset> vector=localPageTable.get(tableName);
            if(vector==null)
                vector=new HashMap<Integer, TablePageOffset>();


            /* for available pag number store its starting recordID endRecordID and offset*/
            vector.put(availablePage,table.get(pageIndex));
            localPageTable.put(tableName,vector);


            /*put newly loaded page and its related table name in global page table*/
            globalPageTable.put(availablePage,tableName);

               randomAccessFile.close();

        }
        catch (Exception e)
        {
		e.printStackTrace();
            //System.out.println("Error opening table file"+e);
        }

		return found;

	}

	 void insertRecord(String tableName, String record)
	{

        String tableNameFile=dataPath.concat("/"+tableName+".csv");
        Vector <TablePageOffset> currentTable=dbInfo.get(tableName);

        int lastPageNo=currentTable.size()-1;
        HashMap <Integer,String> thisPage;

        long freeSpaceInLastPage;
        long fileLength=0;
        String replaceTableName="";
        RandomAccessFile randomAccessFile;

        TablePageOffset lastPageOffset=currentTable.get(lastPageNo);

        //System.out.println("Last page no="+lastPageNo+"Last page starting ID"+ lastPageOffset.startRecord+"last page Ending ID="+lastPageOffset.endRecord);
        try
        {
            randomAccessFile=new RandomAccessFile(tableNameFile,"r");
            fileLength=randomAccessFile.length();
            randomAccessFile.close();

        }
        catch(Exception e)
        {
        	e.printStackTrace();
            //System.out.println("Error while opening file in insertRecord "+e);
        }

            freeSpaceInLastPage=pageSize-(fileLength-lastPageOffset.offSet);

            //System.out.println("File Length="+fileLength+"Record Length="+record.length()+"free space in last page="+freeSpaceInLastPage+"Last page offset="+lastPageOffset.offSet);

            /* if this record can fit into current page*/
            if(freeSpaceInLastPage>=(record.length()+1) || freeSpaceInLastPage==(record.length()) )
            {

                /* make an entry in local page table*/
                int k;
                boolean pageFound=false;

                Object [] pageKey=localPageTable.get(tableName).keySet().toArray();


                /* check whether that page is available in memory */
                for(k=0;k<localPageTable.get(tableName).size();k++)
                {
                    if(localPageTable.get(tableName).get(pageKey[k]).offSet==lastPageOffset.offSet)
                    {
                        pageFound=true;
                        lastPageNo=Integer.parseInt(pageKey[k].toString());
                        break;
                    }
                }


                /* If page found in main memory then just add one more record into that page*/
                if(pageFound)
                {

                   // System.out.println("last page is currently present in memory "+lastPageNo);

                      thisPage=page.get(lastPageNo);
                      thisPage.put(lastPageOffset.endRecord+1,record);
                      page.put(lastPageNo,thisPage);

                    localPageTable.get(tableName).put(lastPageNo,lastPageOffset);

                }

                /* last page is not in momory*/
                else
                {
                   // System.out.println("last page is currently not present in memory");

                    int availablePage=freePageAvailable();


                    /* no free frame available to fetch required page into memory*/
                    if(availablePage==-1)
                    {

                       // System.out.println("free page not found. going for replacement");

                        availablePage=replacePageAlgo();

                    /* get table name from which the old entry is to be invalidated */
                        replaceTableName=globalPageTable.get(availablePage);


                    /* remove old page entry*/

                        localPageTable.get(replaceTableName).remove(availablePage);
                    //    System.out.println("replaced table name="+replaceTableName+" replaced page="+availablePage);

                    }


                    HashMap<Integer,String> buffer=new HashMap<Integer, String>();


                 try
                 {
                    /* open table file to read page */
                    randomAccessFile=new RandomAccessFile(tableNameFile,"r");

                     /* goto first record of that page using offset*/
                    randomAccessFile.seek(lastPageOffset.offSet);

                    String line="";
                    int i;

                     /*read next n records*/
                    for(i=0;;i++)
                    {
                        line=randomAccessFile.readLine();

                        if(line==null)
                            break;

                        buffer.put(lastPageOffset.startRecord+i,line);
                    }
                    buffer.put(lastPageOffset.startRecord+i,record);

                    page.put(availablePage, buffer);

                    randomAccessFile.close();
                    lastPageOffset.endRecord++;

                    /* make entry in global page table*/
                    globalPageTable.put(availablePage,tableName);
                    localPageTable.get(tableName).put(availablePage,lastPageOffset);
                }

                 catch (Exception e)
                 {
                	 e.printStackTrace();
                  //   System.out.println("Error="+e);
                 }
                }


                /* update dbInfo metadata i.e add 1 to endRecord Field*/

                dbInfo.get(tableName).get(dbInfo.get(tableName).size()-1).endRecord++;


                /*write newly inserted record into Table file*/

                 try
                 {
                    // record=record.replaceAll(" ",",");
                     randomAccessFile=new RandomAccessFile(tableNameFile,"rw");
                     randomAccessFile.seek(randomAccessFile.length());
                     randomAccessFile.writeUTF(record+"\n");
                     randomAccessFile.close();
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }

            }


            /* new record can not be fit into current last page*/
            else
            {


               // System.out.println("record can not be fit into current page,require new page");

              int tempOffset,availablePage;
              int tempStart,tempEnd;


                try
                {
                    randomAccessFile=new RandomAccessFile(tableNameFile,"r");
                    fileLength=randomAccessFile.length();
                    randomAccessFile.close();

                }
                catch(Exception e)
                {
                	e.printStackTrace();
                   // System.out.println("Error while opening file in insertRecord "+e);
                }

                tempOffset=(int)fileLength+1;
                tempStart=lastPageOffset.endRecord+1;
                tempEnd=tempStart;

                TablePageOffset tempObj=new TablePageOffset();
                tempObj.offSet=tempOffset;
                tempObj.startRecord=tempStart;
                tempObj.endRecord=tempEnd;

                dbInfo.get(tableName).add(tempObj);

                availablePage=freePageAvailable();

                if(availablePage==-1)
                {
                   // System.out.println("free page not found. going for replacement");

                    availablePage=replacePageAlgo();

                    /* get table name from which the old entry is to be invalidated */
                    replaceTableName=globalPageTable.get(availablePage);


                    /* remove old page entry*/

                    localPageTable.get(replaceTableName).remove(availablePage);
                //    System.out.println("replaced table name="+replaceTableName+" replaced page="+availablePage);


                }


                HashMap<Integer,String> buffer=new HashMap<Integer, String>();

                buffer.put(tempEnd,record);

                page.put(availablePage, buffer);

                    /* make entry in global page table and local page table*/
                globalPageTable.put(availablePage,tableName);
                localPageTable.get(tableName).put(availablePage,tempObj);

                try
                {
                  //  record=record.replaceAll(" ",",");
                    randomAccessFile=new RandomAccessFile(tableNameFile,"rw");
                    randomAccessFile.seek(randomAccessFile.length());
                    randomAccessFile.writeUTF(record+"\n");
                    randomAccessFile.close();
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }


            }

		
	}
	/**
	 *
	 */
	int nextRecordToRead(String tableName)
	{
		pageTable.get(tableName).elementAt(0);
		return 0;

	}
	/**
	 * printDBInfo function prints all global data of all tables
	 */
	    void printDBInfo()
	{
		Iterator<String> it = dbInfo.keySet().iterator();
		Iterator<Vector<TablePageOffset>> pages = dbInfo.values().iterator();
		//System.out.println(dbInfo.size());
		TablePageOffset t ;
		while(it.hasNext())
		{
			System.out.println(it.next());
			Iterator<TablePageOffset> pg = pages.next().iterator();
			while(pg.hasNext())
			{
				t = pg.next();
				System.out.println(t.startRecord + " " + t.endRecord + " " + t.offSet);
			}
		//	it.next();
		}
		
	}
	
  	  void addRecordtoDB(String currentTableName,TablePageOffset offSetObj)
	{
		if(!dbInfo.containsKey((String)currentTableName))/*table name does not present in dbInfo*/
		{
			Vector<TablePageOffset> offsetVector = new Vector<TablePageOffset>();
			offsetVector.add(offSetObj);
			dbInfo.put(currentTableName, offsetVector);
		}
		else /*table name present in dbInfo add element*/
		{
			dbInfo.get(currentTableName).add(offSetObj);
		}		
	}

    public     String searchRecordID(String tableName,int recordID)
    {
        String result=null;
        int i,pageIndex;

        /* get local page table for current table */
        HashMap<Integer,TablePageOffset> localPages=localPageTable.get(tableName);

        if(localPages==null)
            return null;

        /* store all its page numbers already present*/
        Object [] pageNos=localPages.keySet().toArray();

        /* for each pagenumber check if required recordID lies in that page*/
        for(i=0;i<pageNos.length;i++)
        {
            if(localPages.get(pageNos[i]).startRecord <= recordID && recordID <= localPages.get(pageNos[i]).endRecord)
            {

                /* if required recordID present in current page then put that page again to increase its access order in linkedHashMap*/
                page.put(Integer.parseInt(pageNos[i].toString()),page.get(Integer.parseInt(pageNos[i].toString())));

                      break;
            }
        }
          if(i==pageNos.length)
          {
                return null;
          }

        HashMap <Integer,String> recordLine=null;
        recordLine=page.get(pageNos[i]);

        result=recordLine.get(recordID);

        return result;
    }


    public     int freePageAvailable()
    {
        int i;
        for(i=0;i<numberOfPages;i++)
        {
            if(globalPageTable.get(i)==null)
                return i;
        }
        return -1;
    }



     int replacePageAlgo()
    {

        Iterator<Map.Entry<Integer,HashMap<Integer,String>>> it= page.entrySet().iterator();
        Map.Entry<Integer,HashMap<Integer,String>> last=null;
            last=it.next();
        return last.getKey();

    }

}

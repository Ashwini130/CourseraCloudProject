import org.apache.spark.sql.functions.col
var dir = List("1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998",
"1999","2000","2001","2002","2003","2004","2005","2006","2007","2008");
var srcpath = "hdfs://ec2-3-226-217-255.compute-1.amazonaws.com:9000/DATA/";
var dstpath= "hdfs://ec2-3-226-217-255.compute-1.amazonaws.com:9000/Cleaned_Data";


var i= "";
var count = 1;
var df=spark.emptyDataFrame;
for(i <- dir)
{
	var c = 1;
	var File = "/*_"+i+"_*.csv";
	df= spark.read
         .format("csv")
         .option("header", "true") //first line in file has headers
         .load(srcpath+i+File);
	
	val sliceCols = df.columns.slice(0, 55)
	df = df.select(sliceCols.map(name=>col(name)):_*)
	df = df.na.drop(Array("ArrDelay"));
	df.count();
	df.write.option("header","true").csv(dstpath+"/"+i+"_"+c);
	println(i+"_"+c+"Completed!")
	var c = c+1;

}
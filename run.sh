if [ $# -eq 1 ] 
then	
	cd 201305531_src
	javac -cp "./gsp.jar" -d . DBDeliverable2.java DBDeliverable3.java DBSystem.java
	java -cp ".:./gsp.jar" DBDeliverable2 "$1" 
	cd ..
else	
	echo "Usage bash run.sh <config File Path>"
	exit
fi

<?php
/**
 * A PHP wrapper to read/write the key values into constant database (cdb) file format 
 * @author Amitesh Kumar 
 * @email amitesingh@gmail.com
 * @version 0.0.1
 */

/**
 * This class has utility function related to CDB. 
 */
class CDBUtils{
	
	/**
	 * Method to set/update the given key value. 
	 * @param $keyValue : Associative Array, Key => value
	 * @param $mainFile : Main CDB file name to create with updated values
	 * @return {boolean}
	 */
	static public function update($keyValue, $mainFile){
		$isOk = $isMerged = false;
		
		//create a temporary file
		$tmpOnlyUpdateValueCDB = $mainFile . ".update";
		
		$isOk = CDBWriter::write($keyValue, $tmpOnlyUpdateValueCDB);
		if($isOk){
			$isMerged = CDBUtils::merge($tmpOnlyUpdateValueCDB, $mainFile, $mainFile /*final file name */);

    		if($isMerged == false){
    			throw new Exception("Failed : Problem in merging cdb files.");
    		}
    		
			//remove temporary file
    		if(!empty($tmpOnlyUpdateValueCDB) && file_exists($tmpOnlyUpdateValueCDB)){
				unlink($tmpOnlyUpdateValueCDB);
    		}
			
		}
		
		return $isMerged;
	}
	
	/**
	 * Method to merge the two cdb files into one. It will merge the all values in first file.
	 * @param $mainCDBFile
	 * @param $otherCDBFile
	 * @return {boolean}
	 */
	static public function merge($mainCDBFile, $otherCDBFile, $mergedFileName = null){
		if(!file_exists($mainCDBFile)){
			throw new Exception("File not found at $mainCDBFile.");
		}
		
		if(!file_exists($otherCDBFile)){
			throw new Exception("Other CDB file not found.");
		}
		
		$tmpCDBFile  = $mainCDBFile . ".tmp";
		$mainReader  = null;
		$otherReader = null;
		$writer 	 = null;
		$batchSize   = 1000;
		
		$mainReader = new CDBReader();
		$mainReader->open($mainCDBFile);
		
		$otherReader =  new CDBReader();
		$otherReader->open($otherCDBFile);
		
		$writer = new CDBWriter();
		$writer->open($tmpCDBFile, true);
		
		//read from main file and write in the new file.
		$mainReader->resetReader();
		while($keyValues = $mainReader->getInBatch($batchSize)){
			$ret = $writer->setMulti($keyValues);
			if($ret === false){
				throw new Exception("Problem in setting values in cdb file $tmpCDBFile");
			}
		}
		
		//read from other file and check in main file if exists the leave other wise write in the new file.
		$otherReader->resetReader();
		while($keyValues = $otherReader->getInBatch($batchSize)){
			$keys 		= array_keys($keyValues);
			$isExists 	= $mainReader->isExists($keys);
			$kvToAdd 	= array();
			
			foreach ($isExists as $key => $isExist) {
				if($isExist === false){
					$kvToAdd[$key] = $keyValues[$key];
				}
			}
			
			if(count($kvToAdd) > 0){
				$ret = $writer->setMulti($kvToAdd);
				if($ret === false){
					throw new Exception("Problem in setting values in cdb file $tmpCDBFile");
				}
			}
		}
		
		$mainReader->close();
		$otherReader->close();
		$writer->close();
		
		//rename the tmp file to main file.
		if(!empty($mergedFileName) && file_exists($mergedFileName)){
			unlink($mergedFileName);
			rename($tmpCDBFile, $mergedFileName);
		}else{
			rename($tmpCDBFile, $mainCDBFile);
		}
		return true;
	}
	
	/**
	 * This method will show the all content of CDB file.
	 * @param $file : file name
	 */
	static public function dumpCDB($file){
		try{
			$batchSize   = 1000;
		
			$reader = new CDBReader();
			$reader->open($file);
			$reader->resetReader();
			$counter = 0;
			while($keyValues = $reader->getInBatch($batchSize)){
				$size = count($keyValues);
				$counter = $counter + $size;
				echo "\n No of items :$size \n";
				print_r($keyValues);
			}
			
			echo "\n total size : $counter \n";
		}catch(Exception $e){
			echo $e->getMessage();
			print_r($e->getTrace());
		}
	}
	
	static public function testMerge(){
		try{
			$mainFile = "a.cdb";
    		$tmpFile  = "b.cdb";
    		
    		$isMerged = CDBUtils::merge($mainFile, $tmpFile, $mainFile. "_01");
    		if($isMerged){
    			$this->showLog("SUCCESS : cdb files merged.");
    		}else{
    			$this->showLog("Failed : Problem in merging cdb files.");
    		}
		}catch(Exception $e){
			echo $e->getMessage();
			print_r($e->getTrace());
		}
	}
}


/**
 * Class to read the CDB 
 */
class CDBReader{
	private $cdb 	  = null;
	private $fileName = null;
	private $isResetReadPointer = null;
	
	public function __construct($fileName = null){
		$this->fileName = $fileName;
	}
	
	public function __destruct(){
		//TODO : commit the all changes in  main file and close the file pointer
		$this->close();
	}
	
	/**
	 * Method to get the current open CDB file.
	 */
	public function getFileName(){
		return $this->fileName;	
	}
	
	/**
	 * Method to open the CDB file to read.
	 */
	private function openCDB($fileName, $mode = "r"){
		$conn = null;
		$conn = dba_open ($fileName, $mode, "cdb");
		
		if(!$conn || empty($conn)){
			if(!file_exists($fileName)){
				throw new Exception("in openCDB() : $fileName not exists.");
			}else{
				throw new Exception("in openCDB() : Problem in dba_open() for file $fileName");
			}
		}
		
		return $conn;
	}
	
	/**
	 * A public method to open the CDB file to read.
	 */
	public function open($fileName){
		if($this->cdb){
			//file is already opened.
			return;
		}
		
		$this->fileName = empty($fileName) ? $this->fileName : $fileName;
		
		if(empty($this->fileName)){
			throw new Exception("File name is blank.");
		}
		
		$this->cdb  = $this->openCDB($this->fileName);
	}
	
	/**
	 * Method to get the value from CDB for given key.
	 */
	public function get($key){
		if(empty($key)){
			throw new Exception("Problem in get() : key is empty.");
		}
		
		if(is_array($key)){
			return $this->getMulti($key);
		}
		
		return dba_fetch($key, $this->cdb);
	}
	
	/**
	 * Method to get the KVs for given Keys from open CDB
	 */
	public function getMulti($keys){
		$results = array();
		
		if(empty($keys)){
			throw new Exception("Problem in getMulti() : keys is empty.");
		}
		
		if(!is_array($keys)){
			$keys = array($keys);
		}
		
		foreach ($keys as $key) {
			$results[$key] = dba_fetch($key, $this->cdb);
		}

		return $results;
	}
	
	/**
	 * Method to check, Is key is exist or not in CDB
	 */
	public function isExists($key){
		if(empty($key)){
			throw new Exception("Problem in isExists() : key is empty.");
		}
		
		$result = array();
		if(is_array($key)){
			foreach ($key as $k) {
				$result[$k] = dba_exists($k, $this->cdb);
			}
			return $result;
		}
		
		return dba_exists($key, $this->cdb);
		
	}
	
	/**
	 * Close the open CDB file pointer
	 */
	public function close(){
		if(!is_null($this->cdb)){
			dba_close($this->cdb);
			$this->cdb = null;
		}
	}
	
	/**
	 * Method to reset the file reader pointer to start
	 */
	public function resetReader(){
		$this->isResetReadPointer = true;
	}
	
	/**
	 * Method to get the CDB all KVs in batches
	 */
	public function getInBatch($batchSize = 0) {
	    $results = array();
	    $counter = 0;
	    $k 		 = null;
	    
	    if($this->isResetReadPointer){
	    	$k = dba_firstkey($this->cdb);
	    	$this->isResetReadPointer = false;
	    }
	    
	    if($batchSize > 0){
		    for(; $k !== false && $counter < $batchSize; $k = dba_nextkey($this->cdb)) {
		    	if(!empty($k)){
			        $results[$k] = dba_fetch($k, $this->cdb);
			        $counter++;
		    	}
		    }
	    }else{
	    	for(; $k !== false; $k = dba_nextkey($this->cdb)) {
	    		if(!empty($k)){
			        $results[$k] = dba_fetch($k, $this->cdb);
			        $counter++;
	    		}
		    }	
	    }
	    
	    if(empty($results)){
	    	return false;
	    }
	    return $results;
	}
	
	/**
	 * Method to get the values of given keys from given CDB file
	 */
	static public function read($keys, $fileName){
		
		if(empty($keys)){
			throw new Exception("key is blank.");
		}
		
		if(empty($fileName)){
			throw new Exception("File name is blank.");
		}
		
		$obj = new CDBReader();
		$obj->open($fileName);
		$res = $obj->getMulti($keys); 
		$obj->close();
		
		return $res;
	}
	
	static public function doUnitTest(){
		$obj = null;
		echo "Starting...\n";
		try{
			$fileName = "cdbreader_test.cdb";
			$obj = new CDBReader();
			$obj->open($fileName);
			
			$res = $obj->get("key1"); 
			var_export($res);
			
			$res = $obj->getMulti(array("key1", "key2")); 
			var_export($res);
			
			
			$obj->close();
		}catch(Exception $e){
			echo $e->getMessage();
		}
		unset($obj);
		echo "\n Done.\n";
	}
}





/**
 * Class to write the values into CDB file.
 */
class CDBWriter{
	private $cdb 	  = null;
	private $fileName = null;
	
	public function __construct($fileName = null){
		$this->fileName = $fileName;
	}
	
	public function __destruct(){
		//TODO : commit the all changes in  main file and close the file pointer
		$this->close();
	}
	
	/**
	 * Method to get the current open CDB file.
	 */
	public function getFileName(){
		return $this->fileName;	
	}
	
	/**
	 * Method to open the CDB file to read.
	 */
	private function openCDB($fileName, $mode = "n"){
		$conn = null;
		$conn = dba_open ($fileName, $mode, "cdb");
		
		if(!$conn || empty($conn)){
			if(!file_exists($fileName)){
				throw new Exception("in openCDB() : $fileName is not exists.");
			}else{
				throw new Exception("in openCDB() : Problem in dba_open() for file $fileName");
			}
		}
		
		return $conn;
	}
	
	/**
	 * A public method to open the CDB file to read.
	 */
	public function open($fileName, $removeOld = false){
		if($this->cdb){
			//file is already opened.
			return false;
		}
		
		if($removeOld){
			if(file_exists($fileName)){
				 unlink($fileName);
			}
		}
		
		$this->fileName = empty($fileName) ? $this->fileName : $fileName;
		
		if(empty($this->fileName)){
			throw new Exception("File name is blank.");
		}
		
		$this->cdb  = $this->openCDB($this->fileName);
	}
	
	/**
	 * Method to set the KV in CDB file
	 */
	public function set($key, $value){
		if(empty($key)){
			throw new Exception("Problem in set() : key is empty.");
		}
		
		if(is_array($key)){
			return $this->setMulti($key);
		}
		
		return dba_insert ($key, $value, $this->cdb);
	}
	
	/**
	 * Set multiple KVs in CDBs
	 */
	public function setMulti($keys, $values = null){
		$results = array();
		$vals 	 = array();
		
		if(empty($keys)){
			throw new Exception("Problem in setMulti() : keys is empty.");
		}
		
		if(empty($this->cdb)){
			throw new Exception("Problem in setMulti() : CDB handler is empty.");
		}
		
		if(!is_array($keys)){
			$keys = array($keys);
		}
		
		$setCount = 0;
		
		if(is_array($values)){
			$count  = count($keys);
				
			for($i = 0; $i < $count; $i++) {
				if(dba_insert($keys[$i], $values[$i], $this->cdb)){
					$setCount++;
				}
			}
		}else{
			foreach ($keys as $key => $val) {
				if(dba_insert($key, $val, $this->cdb)){
					$setCount++;
				}
			}
		}
		
		if($setCount == 0 && count($keys) != 0 ){
			return false;
		}
		
		return $setCount;
	}
	
	/**
	 * Close the CDB file pointer
	 */
	public function close(){
		if(!is_null($this->cdb)){
			dba_close($this->cdb);
			$this->cdb = null;
		}
	}
	
	/**
	 * Method to write the KVs in given CDB file
	 */
	static public function write($keyValues, $fileName){
		
		if(empty($keyValues)){
			throw new Exception("key is blank.");
		}
		
		if(empty($fileName)){
			throw new Exception("File name is blank.");
		}
		
		$obj = new CDBWriter();
		$obj->open($fileName);
		$res = $obj->setMulti($keyValues); 
		$obj->close();
		
		return $res;
	}
	
	static public function doUnitTest(){
		$obj = null;
		echo "Starting...\n";
		try{
			$fileName = "cdbreader_test.cdb";
			$obj = new CDBWriter();
			$obj->open($fileName);
			
			$res = $obj->set("key1", "value1"); 
			var_export($res);
			
			$res = $obj->setMulti(array("key1" => "value1", "key2" => "value2")); 
			var_export($res);
			
			
			$obj->close();
		}catch(Exception $e){
			echo $e->getMessage();
		}
		unset($obj);
		echo "\n Done.\n";
	}
}

/**
 * @url : http://php.net/manual/en/function.is-array.php
 * 
 * @param $array
 * @return unknown_type
 */
function is_assoc($array) {
  foreach (array_keys($array) as $k => $v) {
    if ($k !== $v)
      return true;
  }
  return false;
}

#CDBWriter::doUnitTest();
#CDBReader::doUnitTest();
#CDBUtils::testMerge();

#CDBUtils::dumpCDB("test.cdb");
?>

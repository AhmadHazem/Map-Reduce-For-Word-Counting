package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"os"
	"sort"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file assigned to this task
	nReduce int, // The number of reduce tasks that will be run
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	/*Read Input file*/
	contents, _ := os.ReadFile(inputFile)

	/*call mapFn function*/
	kvs := mapFn(inputFile, string(contents))

	/*The output of the mapFn will need to be stored in multiple files.
	The number of files equals the number of reduce tasks.*/
	intermediateFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		/*To find the name of the file corresponding to reduce task i,
		call getIntermediateName(jobName, mapTaskNumber, i)*/
		fileName := getIntermediateName(jobName, mapTaskIndex, i)
		tempfile, _ := os.Create(fileName)
		intermediateFiles[i] = tempfile
	}

	/*To know which file you should write a (key, value) pair to, use the hash32 function define in
	the code, but note that it returns a 32-bit integer*/
	for _, kv := range kvs {
		encoder := json.NewEncoder(intermediateFiles[int(hash32(kv.Key))%nReduce])
		encoder.Encode(&kv)
	}

	/*Remember to close the files at the end and to handle any possible errors.*/
	for i := 0; i < nReduce; i++ {
		intermediateFiles[i].Close()
	}

}

func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func runReduceTask(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIndex int, // the index of the reduce task
	nMap int, // the number of map tasks that were run
	reduceFn func(key string, values []string) string,
) {

	/*You can get the names of the files that should be read using the same method that you used
	to generate them during the map phase*/
	intermediateFilesNames := make([]string, nMap)

	GrpMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		intermediateFilesNames[i] = getIntermediateName(jobName, i, reduceTaskIndex)
	}

	/*You can do the grouping while reading from the files*/
	for _, intermediateFilesName := range intermediateFilesNames {
		file, _ := os.OpenFile(intermediateFilesName, os.O_RDWR, 0666)
		decoder := json.NewDecoder(file)
		var kv KeyValue
		for {
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			GrpMap[kv.Key] = append(GrpMap[kv.Key], kv.Value)
		}
		file.Close()
	}

	/*sort keys*/
	keys := make([]string, 0, len(GrpMap))
	for k := range GrpMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	/*The name of the file can abe obtained by getReduceOutName(jobName, reduceTaskIndex)*/
	filename := getReduceOutName(jobName, reduceTaskIndex)
	file, _ := os.Create(filename)
	encoder := json.NewEncoder(file)

	/*calls the user-defined reduce function (reduceFn) for each key*/
	var kvs []KeyValue = make([]KeyValue, 0)
	for _, key := range keys {
		wordFreq := reduceFn(key, GrpMap[key])
		var kv KeyValue
		kv.Key = key
		kv.Value = wordFreq
		kvs = append(kvs, kv)
	}

	/*The output key value pairs should be encoded as Json files as done earlier in the map phase.*/
	for _, kv := range kvs {
		encoder.Encode(&kv)
	}

	file.Close()

}

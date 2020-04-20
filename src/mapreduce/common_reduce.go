package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	var kvsSum []KeyValue
	var kvs []KeyValue
	for i := 0; i < nMap; i++ {
		reduceFileName := reduceName(jobName, i, reduceTask)
		reduceFileNameDesc, err := os.OpenFile(reduceFileName, os.O_RDWR | os.O_CREATE, 0755)
		if err != nil {
			panic(err)
		}
		defer reduceFileNameDesc.Close()
		dec := json.NewDecoder(reduceFileNameDesc)
		dec.Decode(&kvs)
		for _, kv := range kvs {
			debug("key-value's key: %v", kv.Key)
		}
		// 对每一个 decode 后 file 的　kvs 进行合并
		// kvs... 代表对　slice 进行析构
		kvsSum = append(kvsSum, kvs...)
	}

	// refer: https://golang.org/pkg/sort/, 以升序的方式进行排序
	sort.Slice(kvsSum, func(i, j int) bool {
		return kvsSum[i].Key < kvsSum[i].Key
	})
	// 这里需要将对于每个 key 的　value 合并成　{key, []value}
	// call deReduce()

	// write to outFile
	outFileDesc, err := os.OpenFile(outFile, os.O_RDWR | os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	defer outFileDesc.Close()

	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}

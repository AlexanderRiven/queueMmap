package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)


type queueInfo struct {
	FileN   string `json:"fileN"`
	StringN int    `json:"stringN"`
}

func popInfo(f *os.File) (string, int) {
	fi, _ := f.Stat()
	buf := bytes.NewBuffer(make([]byte, 0, fi.Size()))
	f.Seek(0, io.SeekStart)
	io.Copy(buf, f)
	line, err := buf.ReadBytes('\n')
	if err != nil && err != io.EOF {
		fmt.Println("error")
	}
	readQ := queueInfo{}
	json.Unmarshal(line, &readQ)
	return readQ.FileN, readQ.StringN

}

func Enqueue(fil *os.File, value string) {
	filename, stringname := popInfo(fil)
	var f *os.File
	f, err := os.OpenFile(filename + ".json", os.O_RDWR, 0644)
	if errors.Is(err, os.ErrNotExist) {
		size := int64(1093)
		f, err := os.Create(filename + ".json")
		if err != nil {
			log.Fatal("Failed to create output")
		}
		_, err = f.Seek(size-1, 0)
		if err != nil {
			log.Fatal("Failed to seek")
		}
		_, err = f.Write([]byte{0})
		if err != nil {
			log.Fatal("Write failed")
		}
		err = f.Close()
		if err != nil {
			log.Fatal("Failed to close file")
		}
	}



	defer f.Close()

	mmap, _ := mmap.Map(f, mmap.RDWR, 0 )

	defer mmap.Unmap()
	var ptr int
	var ktr int
	var xname int
	xname, _ = strconv.Atoi(filename)
	ptr = stringname

	if stringname + len(value) > len(mmap) {
		//mmap[stringname] = 'g'

		xname, _ = strconv.Atoi(filename)
		f.Close()
		xname += 1
		filename = strconv.Itoa(xname)
		stringname = 0
		ptr = 0
		size := int64(2011)

		f, err := os.Create(filename + ".json")
		if err != nil {
			log.Fatal("Failed to create output")
		}
		_, err = f.Seek(size-1, 0)
		if err != nil {
			log.Fatal("Failed to seek")
		}
		_, err = f.Write([]byte{0})
		if err != nil {
			log.Fatal("Write failed")
		}
		err = f.Close()
		if err != nil {
			log.Fatal("Failed to close file")
		}
	}

	for i := 0; i < len(value); i++{

		mmap[stringname + i] = value[i]
		ktr = i + 1
	}
	ptr = ptr + ktr
	ktr = 0
	mmap.Flush()
	os.Truncate("writeQueue.json", 0)
	newInf := queueInfo{}
	newInf.FileN = strconv.Itoa(xname)
	newInf.StringN = ptr
	newInfM, _ := json.Marshal(newInf)
	err = ioutil.WriteFile("writeQueue.json", newInfM, 0644)
	f.Close()
	mmap.Flush()
	mmap.Unmap()
}



func Dequeue(fil *os.File) (el string) {
	filename, stringname := popInfo(fil)

	f, _ := os.OpenFile(filename + ".json", os.O_RDWR, 0644)

	mmap, _ := mmap.Map(f, mmap.RDWR, 0 )
	defer mmap.Unmap()
	strlen, _ := strconv.Atoi(string(mmap[stringname:stringname+2]))
	if strlen == 0 {
		fmt.Println("Нет записей в очереди")
	}

	el = string(mmap[stringname + 2:stringname + 2 + strlen])

	stringname = stringname + 2 + strlen
	var xname int
	xname, _ = strconv.Atoi(filename)
	f.Close()
	if  (mmap[stringname]) == '\x00' {
		mmap.Unmap()

		e := os.Remove(strconv.Itoa(xname) + ".json")
		if e != nil {
			log.Fatal(e)
		}
		xname, _ = strconv.Atoi(filename)
		xname += 1
		stringname = 0

	}
	mmap.Flush()
	os.Truncate("readQueue.json", 0)
	newInf := queueInfo{}
	newInf.FileN = strconv.Itoa(xname)
	newInf.StringN = stringname
	newInfM, _ := json.Marshal(newInf)
	ioutil.WriteFile("readQueue.json", newInfM, 0644)
	if len(el) == 0 {
		fmt.Println(newInf.FileN)
		fmt.Println(newInf.StringN)
	}
	return el
}

func main() {

	if _, err := os.Stat("0.json"); errors.Is(err, os.ErrNotExist) {
		size := int64(2011)
		fd, err := os.Create("0.json")
		if err != nil {
			log.Fatal("Failed to create output")
		}
		_, err = fd.Seek(size-1, 0)
		if err != nil {
			log.Fatal("Failed to seek")
		}
		_, err = fd.Write([]byte{0})
		if err != nil {
			log.Fatal("Write failed")
		}
		err = fd.Close()
		if err != nil {
			log.Fatal("Failed to close file")
		}
		fd.Close()
	}




	fileQueueRead, err := os.OpenFile("readQueue.json", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("open file error: %v", err)
		return
	}
	fi, _ := fileQueueRead.Stat()
	if fi.Size() == 0 {
		newInf := queueInfo{}
		newInf.FileN = "0"
		newInf.StringN = 0
		newInfM, _ := json.Marshal(newInf)
		err = ioutil.WriteFile("readQueue.json", newInfM, 0644)
	}
	defer fileQueueRead.Close()

	fileQueueWrite, err := os.OpenFile("writeQueue.json", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("open file error: %v", err)
		return
	}
	fi, _ = fileQueueWrite.Stat()
	if fi.Size() == 0 {
		newInf := queueInfo{}
		newInf.FileN = "0"
		newInf.StringN = 0
		newInfM, _ := json.Marshal(newInf)
		err = ioutil.WriteFile("writeQueue.json", newInfM, 0644)
	}
	defer fileQueueWrite.Close()
	var jk string
	_ = jk
	kekmem := "101234567890"
	Enqueue(fileQueueWrite, kekmem)

	for i := 0; i < 200; i++ {

		Enqueue(fileQueueWrite, kekmem)
		Enqueue(fileQueueWrite, kekmem)
		Enqueue(fileQueueWrite, kekmem)
		Enqueue(fileQueueWrite, kekmem)
		Enqueue(fileQueueWrite, kekmem)
		Enqueue(fileQueueWrite, kekmem)
		Enqueue(fileQueueWrite, kekmem)

		jk = Dequeue(fileQueueRead)
		fmt.Println(len(jk))
		fmt.Println(jk)
		jk = Dequeue(fileQueueRead)
		fmt.Println(jk)
		jk = Dequeue(fileQueueRead)
		fmt.Println(jk)
	}
}
